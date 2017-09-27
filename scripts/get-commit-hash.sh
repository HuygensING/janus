#! /bin/sh

mkdir -p target/classes
cat > target/classes/git.properties <<ENDCAT
git.branch=${SOURCE_BRANCH:-"BRANCH_NOT_SET"}
git.commit.id=${SOURCE_COMMIT:-"COMMIT_HASH_NOT_SET"}
git.commit.msg=${COMMIT_MSG:-"COMMIT_MSG_NOT_SET"}
docker.tag=${CACHE_TAG:-"CACHE_TAG_NOT_SET"}
ENDCAT
