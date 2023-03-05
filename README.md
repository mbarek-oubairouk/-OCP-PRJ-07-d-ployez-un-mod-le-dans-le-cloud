# Déployez un modèle dans le cloud

## Mission
- Mettre en place une `architecture Big Data`
- Préparer les données :
   - `Pré-processing`
   - `Réduction de dimension` 
## Contraintes
- Anticiper passage à l'échelle (volume exponentiel, calculs distribués)
- Scripts `PySpark`
- Déploiement cloud 

## Données ([Kaggle](https://www.kaggle.com/datasets/moltean/fruits))

<img src="https://user-images.githubusercontent.com/119690854/222800695-850e7635-7740-4703-89c0-d25b525d3630.png"  width="60%" height="30%">


## Architecture de développement retenue
> `Apache Spark` sur `Amazon Elastic Kubernetes` Service (Amazon EKS)

### Spark et Kubernetes:

<img src="https://user-images.githubusercontent.com/119690854/222803574-b9b80e61-b291-409d-813e-d3e50ef4ba27.png"  width="40%" height="30%">

### Infrastructure As Code (Iac):

<img src="https://user-images.githubusercontent.com/119690854/222803947-c86c8050-84b3-4109-8192-9a3b684e9d2d.png"  width="60%" height="30%">

#### Description du code disponible dans le répertoire `livrables`:
  > `00-conf > conf.ini` : contient les informations d'authentification AWS et déploiement
   >> `mon_bucket` : le nom de la bucket s3 <br>
   >> `aws_id`, `aws_access_key_id`, `aws_secret_access_key` : pour l'authentification AWS

<img src="https://user-images.githubusercontent.com/119690854/222806350-11434837-880a-44ac-9279-86d11497b347.png"  width="90%" height="60%">

## Architecture Cloud retenue

![image](https://user-images.githubusercontent.com/119690854/222807465-aaa208a8-16f7-45a3-b8a7-b18b02bbd0f5.png)

## Chaîne de traitement
 1. Copie des images (`Training`) dans une bucket S3 ( dans mon cas :`s3://oc-mb-fruits/` )
   >  aws s3 sync . `s3://oc-mb-fruits/Training` 
 2. Construction de l'image docker Spark compatible (cloud+kubernetes)
   >  ![image](https://user-images.githubusercontent.com/119690854/222809863-eb583e6c-8fdb-41d0-973d-e4ee8494d95a.png)

 3. Création du cluster, déploiement des noeuds de calculs et la mise à l'échelle.
  > ![image](https://user-images.githubusercontent.com/119690854/222811020-9a18cbe1-8979-4b6c-9341-a6387009b58c.png)

 4. Exécution de l’application Spark
  > Le code de l’application est disponible dans s3 : s3://${mon_bucket}/`app`/`spark-app.py`
  > ![image](https://user-images.githubusercontent.com/119690854/222811458-8ba7069b-838a-47bf-8655-5af5b5c6741f.png)

 5. Monitoring
   - UI Spark 
     ```bash
     # namespace pour driver spark
     NS="spark-fargate"
     # le nom du pod du driver en cours d'exécution
     SPARK_DRIVER_NAME=$(kubectl get pods -n ${NS} | grep  '\-driver' | awk '{print $1}')
     # le forwading des ports 
     kubectl port-forward -n=${NS} ${SPARK_DRIVER_NAME} 4040:4040
     ```
      Navigateur > http://127.0.0.1:4040/
      
      ![image](https://user-images.githubusercontent.com/119690854/222814085-45494464-1e6a-4ec9-9f0f-2fcdb24b8105.png)

   - Dashboard kubernetes
      ```bash
      # dépoiement du dashboard dans le namespace kube-system
      kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.6.1/aio/deploy/recommended.yaml
      # lancé le proxy sur le port 8081
      kubectl proxy --port=8081 &
      ```
      
      Navigateur >  http://127.0.0.1:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy
      
      Pour obtenir le tocken pour se connecter:
      ```bash
      aws eks get-token --cluster-name spark-eks-prod | jq -r '.status.token'
      ```
      
      ![image](https://user-images.githubusercontent.com/119690854/222815648-a69c2c00-e28f-42e8-83c4-7de78d29a652.png)
      ![image](https://user-images.githubusercontent.com/119690854/222815923-daa94c1d-512e-4b8a-a298-aa651b9842f7.png)
      ![image](https://user-images.githubusercontent.com/119690854/222816001-8cbbedfa-0eaf-4a6d-a8de-8162a52ba6f6.png)

  6. Features extraction des images réduites 
      > Le Résultat de la réduction de dimension  est disponible dans s3: `s3://oc-mb-fruits/output/resultats_features_parquet`
      
      ![image](https://user-images.githubusercontent.com/119690854/222820145-99d063d5-9b46-45b8-8f76-165973d33b78.png)


 7. Ordonnancement avec Airflow   
  >  - script installation : `install_aiflow.sh`
  >  - script pipeline : `eks-pipeline.py`
   
  ![image](https://user-images.githubusercontent.com/119690854/222822393-0970370d-150e-43fc-82ad-3c37fcd6e43c.png)

   
   
   
