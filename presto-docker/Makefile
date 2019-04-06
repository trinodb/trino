PRESTO_VERSION := 307

build:
	docker build --build-arg VERSION=${PRESTO_VERSION} -t prestosql/presto-base:${PRESTO_VERSION} base
	docker build --build-arg VERSION=${PRESTO_VERSION} -t prestosql/presto-coordinator:${PRESTO_VERSION} coordinator
	docker build --build-arg VERSION=${PRESTO_VERSION} -t prestosql/presto-worker:${PRESTO_VERSION} worker
	# docker-compose build

.PHONY: test clean

push: build
	docker push prestosql/presto-base:$(PRESTO_VERSION)
	docker push prestosql/presto-coordinator:$(PRESTO_VERSION)
	docker push prestosql/presto-worker:$(PRESTO_VERSION)

run:
	docker-compose up -d
	echo "Please check http://localhost:8080"

down:
	docker-compose down
