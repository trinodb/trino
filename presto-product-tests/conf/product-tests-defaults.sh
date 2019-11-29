export DOCKER_IMAGES_VERSION=${DOCKER_IMAGES_VERSION:-23}

if test -v HADOOP_BASE_IMAGE; then
    test -v TESTS_HIVE_VERSION_MAJOR
else
    test ! -v TESTS_HIVE_VERSION_MAJOR

    export HADOOP_BASE_IMAGE="prestodev/hdp2.6-hive"
    export TESTS_HIVE_VERSION_MAJOR="1"
fi
