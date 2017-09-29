#! /bin/sh

mkdir -p target/classes
cat > target/classes/build.properties <<ENDCAT
docker.img=${IMAGE_NAME:-"IMAGE_NAME_NOT_SET"}
docker.repo=${DOCKER_REPO:-"DOCKER_REPO_NOT_SET"}
docker.tag=${CACHE_TAG:-"CACHE_TAG_NOT_SET"}
git.branch=${SOURCE_BRANCH:-"BRANCH_NOT_SET"}
git.commit.id=${SOURCE_COMMIT:-"COMMIT_HASH_NOT_SET"}
git.commit.msg=${COMMIT_MSG:-"COMMIT_MSG_NOT_SET"}
ENDCAT
