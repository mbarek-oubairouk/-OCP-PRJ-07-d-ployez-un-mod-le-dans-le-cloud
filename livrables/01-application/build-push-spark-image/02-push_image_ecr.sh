
# a rempalce si besoin
region=eu-west-3
AWS_ID=$(aws sts get-caller-identity --query "Account" --output text)
aws ecr get-login-password --region ${region} | docker login --username AWS --password-stdin ${AWS_ID}.dkr.ecr.${region}.amazonaws.com

image_name="spark-py-eks"
docker build -t ${image_name}:prod .


image_id=$(docker images -f reference='${image_name}:prod' | awk '{print $3}')
docker tag  ${AWS_ID}.dkr.ecr.${region}.amazonaws.com/bigdata:${image_name}

# vous devez creer le repo bigdata dans aws ecr avant de publier l"image

docker push ${AWS_ID}.dkr.ecr.${region}.amazonaws.com/bigdata:${image_name}

