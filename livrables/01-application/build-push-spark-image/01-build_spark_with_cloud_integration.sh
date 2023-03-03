

#1. Créez une distribution Spark avec le profil Maven Hadoop Cloud :
 # 1.1	Téléchargez le code source d'Apache Spark (sélectionnez Code source dans le type de package) et extrayez-le.
 # 1.2 ajouter au fichier ,spark-3.3.1/pom.xml, les lignes suivantes:
   # <dependency>
   #     <groupId>org.apache.spark</groupId>
   #     <artifactId>spark-hadoop-cloud_2.12</artifactId>
   #     <version>3.3.1</version>
   #     <scope>provided</scope>
   #  </dependency>


./park-3.3.1/dev/make-distribution.sh --name spark-cloud --pip --tgz -Phadoop-cloud -Phadoop-3.3 -DskipTests \
-Phive -Phive-thriftserver -Pkubernetes


#2. Construire une image de base docker Spark 
dir_spark="spark-3.3.1"

docfile="dist/kubernetes/dockerfiles/spark/bindings/python/Dockerfile"

cd $dir_spark


# cette commande construit deux images docker : spark et spark-py
./bin/docker-image-tool.sh -r barkus  -t  v3.3.1-j11 -p ${docfile} -b java_image_tag=11-jre-slim build

# 3. image spark de base avec les prérequis pour notre projet (voir le Dockerfile)
docker build -t spark-py-eks:prod .