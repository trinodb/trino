FROM lyft/java11:7ea764c41b31e36628bcfeb52d6edaada30b26f4
ARG IAM_ROLE

COPY . /code/prestobase
WORKDIR /code/prestobase

RUN \
     /code/prestobase/mvnw clean install -DskipTests
