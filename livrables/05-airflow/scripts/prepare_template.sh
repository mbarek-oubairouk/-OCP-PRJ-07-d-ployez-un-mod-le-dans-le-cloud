

# le fichier conf.ini avec son chemin
conf_file=$1

if [[ ! -e $conf_file ]]; then
  echo "le fichier $conf_file n'existe pas"
  exit 1
fi

 cat $conf_file |sed "s/\r/\n/g" > $conf_file.tmp
AWS_ACCESS_KEY=$(grep -w aws_access_key_id ${conf_file}.tmp | awk -F= '{print $2}' | base64)
AWS_SECRET_KEY=$(grep -w aws_secret_access_key ${conf_file}.tmp | awk -F= '{print $2}' | base64)
REGION=$(grep -w region_name ${conf_file}.tmp | awk -F= '{print $2}')
CLUSTER_NAME=$(grep -w cluster_name ${conf_file}.tmp | awk -F= '{print $2}')
dir_liv=$(grep -w livrables ${conf_file}.tmp | awk -F= '{print $2}')

rm -f $conf_file.tmp 2>/dev/null
AWS_ID=$(aws sts get-caller-identity --query "Account" --output text)


if [[ ! -d "${dir_liv}" ]]; then
  echo "le répertoire #${dir_liv}# n'existe pas"
  exit 1
fi

# remplacement de variable dans les templates yaml

for file in $(find ${dir_liv} -type f -name "*.yaml" -not -path "*/old/*")
do

grep -E '\${AWS_ACCESS_KEY}|\${AWS_SECRET_KEY}|\${AWS_ID}|\${CLUSTER_NAME}|\${REGION}' $file >/dev/null 2>&1
RC=$?
if [[ $RC -eq 0 ]]; then
      tf=$(basename $file)
      cat $file |sed "s/\${AWS_ACCESS_KEY}/${AWS_ACCESS_KEY}/g;s/\${AWS_SECRET_KEY}/${AWS_SECRET_KEY}/g; \
                s/\${AWS_ID}/${AWS_ID}/g;s/\${CLUSTER_NAME}/${CLUSTER_NAME}/g;s/\${REGION}/${REGION}/g" > ${dir_liv}/05-airflow/templates/${tf}
fi
done