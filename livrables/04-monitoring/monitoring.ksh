
# uniquement pour le cloud aws
##########################################################################################
#                                      kubernetes dashboard                              #
##########################################################################################
#kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.6.1/aio/deploy/recommended.yaml

kubectl proxy --port=8081 &
echo "tocken:"
tocken=$(aws eks get-token --cluster-name spark-eks-prod | jq -r '.status.token')
echo $tocken

echo "http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy"

##########################################################################################
#                                          UI Spark                                      #
##########################################################################################

NS="spark-fargate"

SPARK_DRIVER_NAME=$(kubectl get pods -n ${NS} | grep  '\-driver' | awk '{print $1}')
RC=$?

while [[ $RC -ne 0 ]]; do
    SPARK_DRIVER_NAME=$(kubectl get pods -n ${NS} | grep  '\-driver' | awk '{print $1}')
done
kubectl port-forward -n=${NS} ${SPARK_DRIVER_NAME} 4040:4040
    echo "UI SPARK:"
    echo  http://127.0.0.1:4040/


