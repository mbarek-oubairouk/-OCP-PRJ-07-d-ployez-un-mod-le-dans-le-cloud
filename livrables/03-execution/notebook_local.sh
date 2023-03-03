
# execution du notebook dans l'image docker en local
id_image=$(docker images spark-py-eks:prod  --format "{{.ID}}")
vol_host="/mnt/c/developpement/wsl/pro8/livrables/01-application/code-source/"
vol_dock="/opt/spark/work-dir/app/"

conf_file="../00-conf/conf.ini"

AWS_ACCESS_KEY=$(grep -w aws_access_key_id ${conf_file} | awk -F= '{print $2}')
AWS_SECRET_KEY=$(grep -w aws_secret_access_key ${conf_file} | awk -F= '{print $2}')

docker run --name notebook_jup -p 8888:8888 -p 4040:4040 -e JUPYTER_TOKEN=easy -e aws_key=$AWS_ACCESS_KEY -e aws_secret=$AWS_SECRET_KEY \
-v $vol_host:$vol_dock  -it $id_image jupyter notebook  --no-browser --ip=0.0.0.0