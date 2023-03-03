
CLUSTER_NAME="spark-eks-prod"

dir_liv="/home/adamin/pro8/livrables"

kubectl delete -f $dir_liv/05-airflow/templates/02-cluster_autoscaler.yaml

eksctl delete cluster -n $CLUSTER_NAME


