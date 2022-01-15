#!/bin/bash
set -e
#设置IMAGE_NAME
export IMAGE_NAME=registry-jinan-lab.inspurcloud.cn/drdb/znbasedb/znbase
export DOCKER_REGISTRY_USERNAME=inspurclouddev.drdb1
export DOCKER_REGISTRY_PASSWORD=6tnhfw62hlzdtfgk6tp72bbk9sgzn9p7
export DOCKER_REGISTRY=registry-jinan-lab.inspurcloud.cn

cd $PROJECT_SITE/znbase
#回退前面节点对源码的更改
git reset --hard
git clean -df
#生成编译，打镜像脚本
sudo chown -R vagrant:vagrant ../znbase
./build/builder.sh make build
cp znbase ./build/deploy/
time_tag=`date "+%Y%m%d_%H%M%S"`
last_commit_id="`git rev-parse --short HEAD`"

docker login $DOCKER_REGISTRY -u $DOCKER_REGISTRY_USERNAME -p $DOCKER_REGISTRY_PASSWORD
docker build -f ./build/deploy/Dockerfile -t $IMAGE_NAME:$time_tag ./build/deploy
docker tag $IMAGE_NAME:$time_tag $IMAGE_NAME:$last_commit_id
docker push $IMAGE_NAME:$time_tag
docker push $IMAGE_NAME:$last_commit_id