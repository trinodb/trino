# Docker PrestoSQL Cluster

## Build image

```
$ make build

## make push
```

## Launch presto

Presto cluster can be launched by using docker-compose.

```
$ make run
```

## docker-compose.yml

Images will be uploaded in [DockerHub](https://hub.docker.com/prestosql). 
These images are build with the corresponding version of Presto. 
`command` is required to pass node id information which must be unique in a cluster.

### Run

```
$ docker-compose up -d
```

# Contact the awesome Presto community to learn more about PrestoSQL

User Mailing List: [presto-users](https://groups.google.com/group/presto-users)
Users & Development Slack: [prestosql.slack.com](https://join.slack.com/t/prestosql/shared_invite/enQtNTMyNzU2NzQ1NzQ4LWRhMTE4ZTA4NjM0NDA1NmFkZjEyZDJmN2MxNGY1ZTk4NmM4MzMxZDk4OGQ0NjZhNmQxMWUyNGIxMDliODk0MmU) 
Twitter: @prestosql #prestosql #prestodb
YouTube: Presto Channel