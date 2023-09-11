#Jenkins Shell Script - Start

#Project Name
project_name=crc_worker

if [ "$environment" = "uat" ]
then
  export KUBECONFIG=/var/lib/jenkins/config-uat
elif
  [ "$environment" = "qa" ]
then
  export KUBECONFIG=/var/lib/jenkins/config-qa
else
  export KUBECONFIG=/var/lib/jenkins/config
fi

#Delete the Existing Files and Clone Again

rm -rf ./$project_name
git clone -b $branch --single-branch --depth 1 git@<username>/$project_name.git || true
cd $project_name

git checkout $branch

#Pull Source Code from $branch
git pull

$(aws ecr get-login --no-include-email --region ap-south-1)
#Build Docker Image, Tag and Push to Private Docker Registry
docker build -t betterplace/crc-worker .
docker tag betterplace/crc-worker:latest 372004264077.dkr.ecr.ap-south-1.amazonaws.com/betterplace/crc-worker:$environment
docker push 372004264077.dkr.ecr.ap-south-1.amazonaws.com/betterplace/crc-worker:$environment

#Removing the docker image from the local
if [ $? -eq 0 ]
then
docker rmi -f betterplace/crc-worker
docker rmi -f 372004264077.dkr.ecr.ap-south-1.amazonaws.com/betterplace/crc-worker:$environment

#Deploying Docker Image as Pod in Kubernetes
kubectl config use-context $environment
kubectl apply -f k8s/$environment/

kubectl rollout restart -f k8s/$environment/$project_name-deployment.yaml
