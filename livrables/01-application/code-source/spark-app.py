# Chargement des librairies
import datetime
import io
import sys
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
import warnings
warnings.filterwarnings('ignore')

import time
from datetime import datetime
import pandas as pd
import numpy as np
from typing import Iterator
# Visualisation
import matplotlib
import matplotlib.pyplot as plt

# Pyspark
import pyspark
from pyspark.sql.functions import element_at, split, col, pandas_udf, PandasUDFType, udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession

# Tensorflow Keras
import tensorflow as tf
from tensorflow.keras.applications.inception_v3 import InceptionV3, preprocess_input
from tensorflow.keras.preprocessing.image import img_to_array, load_img

# Gestion des images
import PIL
from PIL import Image

# Taches ML
from pyspark.ml.image import ImageSchema

# Réduction de dimension - PCA
from pyspark.ml.feature import PCA
from pyspark.ml.feature import StandardScaler
from pyspark.ml.linalg import Vectors, VectorUDT, DenseVector

# Modélisation
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier, LogisticRegression, RandomForestClassifier, NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

# Matrice de confusion

from pyspark.mllib.evaluation import MulticlassMetrics
import itertools

# Versions
print('Version des librairies utilisées :')
print('Python        : ' + sys.version)
print('tensorflow    : ' + tf.__version__)
print('pyspark       : ' + pyspark.__version__)
print('PIL           : ' + PIL.__version__)
print('Numpy         : ' + np.__version__)
print('Pandas        : ' + pd.__version__)
print('Matplotlib    : ' + matplotlib.__version__)
now = datetime.now().isoformat()
print('Lancé le      : ' + now)


## Define the details of the SparkSession

spark = SparkSession.builder.appName('FeatExtraction').getOrCreate()

############################################

nom_bucket="oc-mb-fruits"

# Chemin de stockage des images du jeu de données
path_train_set = f"s3a://{nom_bucket}/Training/*/*"

# Chargement des images du train set au format "binaryFile"
df_binary_train = spark.read.format("binaryFile") \
  .option("pathGlobFilter", "*.jpg") \
  .option("recursiveFileLookup", "true") \
  .load(path_train_set)

# Schéma ?
df_binary_train.printSchema()

# Nombre d'images?
df_binary_train.count()

# Visualisation des 20 premières images
df_binary_train.show()

#### Labellisation - extraction de la classe de l'image

# Ajout dans la colonne Classe pour chaque image traitée de l'avant dernier
# élément du nom du répertoire de stockage de l'image==>df_binary_train["path"]
df_binary_train = df_binary_train.withColumn("Classe", element_at(split(df_binary_train["path"], "/"), -2))

# Schéma ?
df_binary_train.printSchema()

# Visualisation des 20 premières images avec la classe
df_binary_train.show()

## Extraction des features importantes pour chaque image
"""
- Comme vu lors du projet 6, l'extraction des features par transfert learning donne des résultats plus performants que les méthodes anciennes (ORB, SIFT). 
- Nous allons donc extraire les features les plus importantes pour la classification de nos images en utilisant un modèle **[InceptionV3](https://www.researchgate.net/figure/Schematic-diagram-of-the-Inception-v3-model-based-on-convolutional-neural-networks_fig3_337200783)** de deep learning pré-entrainé sur de la classification d'images.
- Comme le but de ce projet n'est pas d'effectuer la classification; La dernière couche (softmax), qui effectue la classification, est supprimée à l'aide du paramètre (include_top=False). Cela nous permettra de choisir un modèle de classification adapté à nos classes.
"""
### Préparation du dataframe de travail

"""
Le dataframe de travail sera composé des colonnes utiles à partir du dataframe des images binaires :
- le répertoire de stockage de l'image (colonne path),
- le label (colonne Classe) de chaque image,
- les features les plus importantes ajoutées après exécution du modèle (étape 3.3.).
"""

df_images = df_binary_train.select("path", "Classe")
df_images.show()


## Préparation du modèle InceptionV3

"""
Utilisation de la technique de transfert learning pour extraire les features de chaque image avec le modèle **[InceptionV3](https://keras.io/api/applications/inceptionv3/)** de la librairie Keras de tensorflow avec l'aide des recommandations de databricks sur l'utilisation du transfert learning.

***Note : Recommandation de databricks***
***
- Pandas UDFs on large records (e.g., very large images) can run into Out Of Memory (OOM) errors.
  If you hit such errors in the cell below, try reducing the Arrow batch size via `maxRecordsPerBatch`.
`spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "1024")`
- Pour les modèles de taille modérée (< 1 Go, c'est le cas de notre projet), une bonne pratique consiste à télécharger le modèle vers le pilote Spark, puis à diffuser les poids aux travailleurs. Ce carnet de notes utilise cette approche.
 `bc_model_weights = sc.broadcast(model.get_weights())`
 `model.set_weights(bc_model_weights.value)`

# [Source](https://docs.databricks.com/_static/notebooks/deep-learning/deep-learning-transfer-learning-keras.html)
"""

# Instanciation du modèle
model = InceptionV3(
        include_top=False,  # Couche softmax de classification supprimée
        weights=None,  # Poids pré-entraînés sur s3
        input_shape=(100,100,3), # Image de taille 100x100 en couleur (channel=3)
        pooling='max' # Utilisation du max de pooling
)

# chargement des poids du modèle inception_v3_weights_tf_dim_ordering_tf_kernels_notop.h5
poids_model = "/opt/spark/work-dir/app/model/inception-v3-weights-tf-dim-ordering-tf-kernels-notop.h5"
model.load_weights(poids_model)



# Description des caractéristiques du modèle
model.summary()


"""
Le vecteur des features est de dimension de chaque image à la dimensions (1, 1, 2048)

# Extraction des features pour chaque image

# [Source_databricks](https://docs.databricks.com/_static/notebooks/deep-learning/deep-learning-transfer-learning-keras.html)

#  Fonctions utiles à l'extraction des features
"""
# Préparation du modèle


# Permettre aux workers Spark d'accéder aux poids utilisés par le modèle
bc_model_weights = spark.sparkContext.broadcast(model.get_weights())


def model_fn():
  """
  Renvoie un modèle Inception3 avec la couche supérieure supprimée et les poids pré-entraînés sur imagenet diffusés.
  """
  model = InceptionV3(
        include_top=False,  # Couche softmax de classification supprimée
        weights=None,  # Poids pré-entraînés sur Imagenet
#         input_shape=(100,100,3), # Image de taille 100x100 en couleur (channel=3)
        pooling='max' # Utilisation du max de pooling
  )
  model.set_weights(bc_model_weights.value)
  
  return model



# Fonction de redimensionnement de l'image
#  - Les images à transmettre en entrée de InceptionV3 doivent entre de dimension (299,299, 3)



# Redimensionnement des images en 299x299
def preprocess(content):
    """
    Prétraite les octets de l'image brute pour la prédiction.
    param : content : objet image, obligatoire
    return : image redimensionnée en Array
    """
    # lecture + redimension (299x299) pour Xception
    img = PIL.Image.open(io.BytesIO(content)).resize([299, 299])
    # transforme l'image en Array     
    arr = img_to_array(img)
    return preprocess_input(arr)




# Extraction des features par le modèle dans un vecteur
def featurize_series(model, content_series):
  """
  Featurise une pd.Series d'images brutes en utilisant le modèle d'entrée.
  param : 
    model : modèle à utiliser pour l'extraction, obligatoire.
    content_series : image redimensionnée (299, 299, 3) en Array
  :return: les features importantes de l'image en pd.Series.
  """
  input = np.stack(content_series.map(preprocess))
  # Prédiction du modèle
  preds = model.predict(input)
  # Pour certaines couches, les caractéristiques de sortie seront des tenseurs multidimensionnels.
  # Nous aplatissons les tenseurs de caractéristiques en vecteurs pour faciliter le stockage dans
  # les DataFrames de Spark.
  output = [p.flatten() for p in preds]
  
  return pd.Series(output)



#@pandas_udf('array<float>', PandasUDFType.SCALAR_ITER)
#def featurize_udf(content_series_iter):
@pandas_udf('array<float>')
def featurize_udf(content_series_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
  '''
  Cette méthode est un Scalar Iterator pandas UDF enveloppant notre fonction de featurisation.
  Le décorateur spécifie que cette méthode renvoie une colonne Spark DataFrame de type ArrayType(FloatType).
  
  :param content_series_iter : Cet argument est un itérateur sur des lots de données, où chaque lot est une série pandas de données d'image.
  '''
  # With Scalar Iterator pandas UDFs, we can load the model once and then re-use it
  # for multiple data batches.  This amortizes the overhead of loading big models.
  model = model_fn()
  for content_series in content_series_iter:
    yield featurize_series(model, content_series)



# Extraction des features pour chaque image du dataframe



# Les UDF de Pandas sur de grands enregistrements (par exemple, de très grandes images) peuvent rencontrer des erreurs de type Out Of Memory (OOM).
# Si vous rencontrez de telles erreurs dans la cellule ci-dessous, essayez de réduire la taille du lot Arrow via `maxRecordsPerBatch`.

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "1024")



# Nous pouvons maintenant exécuter la featurisation sur l'ensemble de notre DataFrame Spark.
# REMARQUE : Cela peut prendre beaucoup de temps (environ 10 minutes) car il applique un grand modèle à l'ensemble des données.
features_df = df_binary_train.repartition(16).select(col("path"), col('Classe'), featurize_udf("content").alias("features"))


# comptage images?
print(features_df.count())



# Réduction de dimension - Principal Component Analysis
  
def preprocess_pca(dataframe):
  '''
     Préparation des données :
     - transformation en vecteur dense
     - standardisation
     param : dataframe : dataframe d'images
     return : dataframe avec features vecteur dense standardisé
  '''
  
  # Préparation des données - conversion des données images en vecteur dense
  transform_vecteur_dense = udf(lambda r: Vectors.dense(r), VectorUDT())
  dataframe = dataframe.withColumn('features_vectors', transform_vecteur_dense('features'))
  
  # Standardisation obligatoire pour PCA
  scaler_std = StandardScaler(inputCol="features_vectors", outputCol="features_scaled", withStd=True, withMean=True)
  model_std = scaler_std.fit(dataframe)
  # Mise à l'échelle
  dataframe = model_std.transform(dataframe)
  
  return dataframe



# Recherche du nombre de composante expliquant 95% de la variance



def recherche_nb_composante(dataframe, nb_comp=400):
    '''
       Recherche d nombre de composante expliquant 95% de la variance
       param : dataframe : dataframe d'images
       return : k nombre de composante expliquant 95% de la variance totale
    '''
    
    pca = PCA(k = nb_comp,
              inputCol="features_scaled", 
              outputCol="features_pca")
 
    model_pca = pca.fit(dataframe)
    variance = model_pca.explainedVariance
 
    # visuel
    plt.plot(np.arange(len(variance)) + 1, variance.cumsum(), c="red", marker='o')
    plt.xlabel("Nb composantes")
    plt.ylabel("% variance")
    plt.show(block=False)
 
    def nb_comp ():
      for i in range(len(variance)):
          a = variance.cumsum()[i]
          if a >= 0.95:
              print("{} composantes principales expliquent au moins 95% de la variance totale".format(i))
              break
      return i
 
    k=nb_comp()
  
    return k


# Pré-processing (vecteur dense, standardisation)
df_pca = preprocess_pca(features_df)

# Nombre de composante expliquant 95% de la variance
n_components = recherche_nb_composante(df_pca)

# 325 composantes expliquent plus de 90% de la variance
#n_components = 325



# Réduction de dimension PCA



# Entrainement de l'algorithme
pca = PCA(k=n_components, inputCol='features_scaled', outputCol='vectors_pca')
model_pca = pca.fit(df_pca)



# Transformation des images sur les k premières composantes
df_reduit = model_pca.transform(df_pca)


# Visualisation du dataframe réduit
df_reduit.show(20)



# Sauvegarde des données



# Finalement, on sauvegarde les données pré-traitées et réduites au format parquet.
 
path_parquet = f"s3a://{nom_bucket}/output"

df_reduit.write.mode("overwrite").parquet(f"{path_parquet}/resultats_features_parquet")




# Test de classification

# Préparation des données

# Nombre aléatoire pour la reproductibilité des résultats
seed = 21

# Dataframe de travail



# Chargement du dataframe sauvegardé en parquet
parquetFiles = f"{path_parquet}/resultats_features_parquet/"

df_reduit = spark.read.parquet(parquetFiles)



# Conservation de la classe de l'image et des vecteurs pca
data = df_reduit[["Classe", "vectors_pca"]]



data.show(5)


# Encodage de la variable cible : la classe de l'image acceptable par le modèle
indexer = StringIndexer(inputCol="Classe", outputCol="Classe_index")

# Fit the indexer to learn Classe/index pairs
indexerModel = indexer.fit(data)

# Append a new column with the index
data = indexerModel.transform(data)

print("prédiction > Encodage de la variable cible <Classe>:")
data.show(20)


# Découpage du jeu du train set en jeux d'entraînement et de validation

# data splitting
(train_data, valid_data) = data.randomSplit([0.8, 0.2])



print("Nbre élément train_data : " + str(train_data.count()))
print("Nbre élément valid_data : " + str(valid_data.count()))



train_data.show(3)



# Modélisation Logistic Regression

# [Source](https://spark.apache.org/docs/latest/ml-classification-regression.html#multinomial-logistic-regression)

# Instanciation du modèle.
lr = LogisticRegression(labelCol="Classe_index", featuresCol="vectors_pca",
                        maxIter=5)

# Entraînement du modèle
lr_model = lr.fit(train_data)

# Make predictions.
lr_predictions = lr_model.transform(valid_data)

# Select example rows to display.
lr_predictions.select("prediction", "Classe_index").show(5)

# Évaluation du modèle

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="Classe_index", predictionCol="prediction", metricName="accuracy")
lr_accuracy = evaluator.evaluate(lr_predictions)
print("[lr] Test Error = %g" % (1.0 - lr_accuracy))
print("[lr] Accuracy = %g " % lr_accuracy)



# Informations sur le modèle

# Print the coefficients and intercept for multinomial logistic regression
print("[lr] Coefficients: \n" + str(lr_model.coefficientMatrix))
print("[lr] Intercept: " + str(lr_model.interceptVector))

trainingSummary = lr_model.summary

# Obtain the objective per iteration
objectiveHistory = trainingSummary.objectiveHistory
print("[lr] objectiveHistory:")
for objective in objectiveHistory:
    print(objective)

# for multiclass, we can inspect metrics on a per-label basis
print("False positive rate by label:")
for i, rate in enumerate(trainingSummary.falsePositiveRateByLabel):
    print("label %d: %s" % (i, rate))

print("True positive rate by label:")
for i, rate in enumerate(trainingSummary.truePositiveRateByLabel):
    print("label %d: %s" % (i, rate))

print("Precision by label:")
for i, prec in enumerate(trainingSummary.precisionByLabel):
    print("label %d: %s" % (i, prec))

print("Recall by label:")
for i, rec in enumerate(trainingSummary.recallByLabel):
    print("label %d: %s" % (i, rec))

print("F-measure by label:")
for i, f in enumerate(trainingSummary.fMeasureByLabel()):
    print("label %d: %s" % (i, f))

accuracy = trainingSummary.accuracy
falsePositiveRate = trainingSummary.weightedFalsePositiveRate
truePositiveRate = trainingSummary.weightedTruePositiveRate
fMeasure = trainingSummary.weightedFMeasure()
precision = trainingSummary.weightedPrecision
recall = trainingSummary.weightedRecall
print("Accuracy: %s\nFPR: %s\nTPR: %s\nF-measure: %s\nPrecision: %s\nRecall: %s"
      % (accuracy, falsePositiveRate, truePositiveRate, fMeasure, precision, recall))



# Modélisation Decision Tree Classifier

# [Source](https://spark.apache.org/docs/latest/ml-classification-regression.html#decision-tree-classifier)

# Instanciation du modèle.
dtc = DecisionTreeClassifier(labelCol="Classe_index", featuresCol="vectors_pca",
                             seed=seed)

# Entraînement du modèle
dtc_model = dtc.fit(train_data)

# Make predictions.
dtc_predictions = dtc_model.transform(valid_data)

# Select example rows to display.
dtc_predictions.select("prediction", "Classe_index").show(5)


# Évaluation du modèle


# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="Classe_index", predictionCol="prediction", metricName="accuracy")
dtc_accuracy = evaluator.evaluate(dtc_predictions)
print("[dt] Test Error = %g" % (1.0 - dtc_accuracy))
print("[dt] Accuracy = %g " % dtc_accuracy)


# Informations sur le modèle

print(dtc_model.toDebugString)



# Modélisation Random Forest Classifier


# [Source](https://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-classifier)


# Instanciation du modèle.
rf = RandomForestClassifier(labelCol="Classe_index", featuresCol="vectors_pca", numTrees=20,
                             seed=seed)

# Entraînement du modèle
rf_model = rf.fit(train_data)


# Make predictions.
rf_predictions = rf_model.transform(valid_data)

# Select example rows to display.
rf_predictions.select("prediction", "Classe_index").show(5)


# Évaluation du modèle

# Select (prediction, true label) and compute test error
rf_evaluator = MulticlassClassificationEvaluator(
    labelCol="Classe_index", predictionCol="prediction", metricName="accuracy")
rf_accuracy = rf_evaluator.evaluate(rf_predictions)
print("[dt] Test Error = %g" % (1.0 - rf_accuracy))
print("[dt] Accuracy = %g " % rf_accuracy)


# Matrice de confusion

def plot_confusion_matrix(cm, classes,
                          normalize=False,
                          title='Confusion matrix',
                          cmap=plt.cm.GnBu):

    plt.imshow(cm, interpolation='nearest', cmap=cmap)
    plt.title(title)
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes, rotation=45)
    plt.yticks(tick_marks, classes)

    fmt = '.2f' #if normalize else 'd'
    thresh = cm.max() / 2.

    for i, j in itertools.product(range(cm.shape[0]), 
                                  range(cm.shape[1])):
        plt.text(j, i, format(cm[i, j], fmt),
                 horizontalalignment="center",
                 color="white" if cm[i, j] > thresh else "black")


    plt.tight_layout()
    plt.ylabel('True label')
    plt.xlabel('Predicted label')


lrmetrics = MulticlassMetrics(rf_predictions['Classe_index','prediction'].rdd)
cnf_matrix = lrmetrics.confusionMatrix()
print('[rf] Confusion de Matrix:\n', cnf_matrix.toArray())


plt.figure(figsize=(7,7))
plt.grid(False)

# call pre defined function
plot_confusion_matrix(cnf_matrix.toArray(), classes=range(len(cnf_matrix.toArray())))

# Informations sur le modèle***

print(rf_model.toDebugString)
