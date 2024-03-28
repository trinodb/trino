
# Logging

Trino include numerous features to better understand and monitor a running
system. Logging and configuring theis one important aspectin Trino log.properties

java package space

log level

(log-levels)=
## Log levels

The optional log levels file, `etc/log.properties`, allows setting the
minimum log level for named logger hierarchies. Every logger has a name,
which is typically the fully qualified name of the class that uses the logger.
Loggers have a hierarchy based on the dots in the name, like Java packages.
For example, consider the following log levels file:

```text
io.trino=INFO
```

This would set the minimum level to `INFO` for both `io.trino.server` and
`io.trino.plugin.hive`. The default minimum level is `INFO`, thus the above
example does not actually change anything. There are four levels: `DEBUG`,
`INFO`, `WARN` and `ERROR`.





properties-logging


The logging stuff is a little more involved
11:20
There are 2 features that are orthogonal, the first is json logging, the second it tcp channel logging. We use both in galaxy.
11:24
For json logging, you can add log.format=json to your config.properties file and optionally also set node.annotations-file to provide static annotations to the emitted logs,  that file can contain something like the following:
host_ip=1.2.3.4
service_name=trino
pod_name=${ENV:HOSTNAME}
It supports environment variable substitution, so this comes in handy when running in Kubernetes, as the pod name is the host name. This means every log line now has this pod_name attached to it.
11:25
The tcp logging allows you to log to a tcp socket instead of a file, there are some retries and buffering built-in, but it assumes that the target is reliable and can cause some problems if the target is flaky. The way to use it is to use the log.path=tcp://<server_ip>:<server_port> syntax.
11:29
Bringing both of those together in an example, we can install fluent-bit locally with brew install fluent-bit then start it with the following config file:
pipeline:
  inputs:
  - name: tcp
    tag: trino
    listen: 0.0.0.0
    port: 5170
    buffer_size: 2048
    format: json
  outputs:
  - name: stdout
    match: '*'
and the command fluent-bit -c <a file with the above contents>
Then we start up Trino with the following added to config.properties :
log.path=tcp://localhost:5170
log.format=json

node.annotation-file=etc/annotations.properties
and the etc/annotation.properties file like
host_ip=1.2.3.4
service_name=trino
pod_name=${ENV:HOSTNAME}
then you'll see in the fluent-bit window the structured log lines being emitted. If you wanted then to point fluentbit to your logging system of choice, just follow any of the output plugin guides here : https://docs.fluentbit.io/manual/pipeline/outputs
docs.fluentbit.io
Outputs (22 kB)
https://docs.fluentbit.io/manual/pipeline/outputs

mattstep
  11:32 AM
It might be good to also point to this doc or incorporate it, you can use the environment variable substitution along with kubernetes deployments to add all sorts of annotation.properties keys based on metadata in kubernetes : https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/
kubernetes.iokubernetes.io
Expose Pod Information to Containers Through Environment Variables
This page shows how a Pod can use environment variables to expose information about itself to containers running in the Pod, using the downward API. You can use environment variables to expose Pod fields, container fields, or both.
In Kubernetes, there are two ways to expose Pod and container fields to a running container:
Environment variables, as explained in this task Volume files Together, these two ways of exposing Pod and container fields are called the downward API.
11:34
Something like the following annotations.properties would correspond with that kubernetes.io page:
node_name=${ENV:MY_NODE_NAME}
pod_name=${ENV:MY_POD_NAME}
pod_namespace=${ENV:MY_POD_NAMESPACE}








