# Trino on Kubernetes with Helm

[Kubernetes](https://kubernetes.io) is a container orchestration platform that
allows you to deploy Trino and other applications in a repeatable manner across
different types of infrastructure. This can range from deploying on your laptop
using tools like [kind](https://kind.sigs.k8s.io), to running on a managed
Kubernetes service on cloud services like
[Amazon Elastic Kubernetes Service](https://aws.amazon.com/eks),
[Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine),
[Azure Kubernetes Service](https://azure.microsoft.com/services/kubernetes-service),
and others.

The fastest way to run Trino on Kubernetes is to use the
[Trino Helm chart](https://github.com/trinodb/charts).
[Helm](https://helm.sh) is a package manager for Kubernetes applications that
allows for simpler installation and versioning by templating Kubernetes
configuration files. This allows you to prototype on your local or on-premise
cluster and use the same deployment mechanism to deploy to the cloud to scale
up.

## Requirements

- A Kubernetes cluster with a
  [supported version](https://kubernetes.io/releases/) of Kubernetes.

  - If you don't have a Kubernetes cluster, you can
    {ref}`run one locally using kind <running-a-local-kubernetes-cluster-with-kind>`.

- [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) with a version
  that adheres to the
  [Kubernetes version skew policy](https://kubernetes.io/releases/version-skew-policy/)
  installed on the machine managing the Kubernetes deployment.

- [helm](https://helm.sh) with a version that adheres to the
  [Helm version skew policy](https://helm.sh/docs/topics/version_skew/)
  installed on the machine managing the Kubernetes deployment.

(running-trino-using-helm)=

## Running Trino using Helm

Run the following commands from the system with `helm` and `kubectl`
installed and configured to connect to your running Kubernetes cluster:

1. Validate `kubectl` is pointing to the correct cluster by running the
   command:

   ```text
   kubectl cluster-info
   ```

   You should see output that shows the correct Kubernetes control plane
   address.

2. Add the Trino Helm chart repository to Helm if you haven't done so already.
   This tells Helm where to find the Trino charts. You can name the repository
   whatever you want, `trino` is a good choice.

   ```text
   helm repo add trino https://trinodb.github.io/charts
   ```

3. Install Trino on the Kubernetes cluster using the Helm chart. Start by
   running the `install` command to use all default values and create
   a cluster called `example-trino-cluster`.

   ```text
   helm install example-trino-cluster trino/trino
   ```

   This generates the Kubernetes configuration files by inserting properties
   into helm templates. The Helm chart contains
   [default values](https://trinodb.github.io/charts/charts/trino/)
   that can be overridden by a YAML file to update default settings.

   1. *(Optional)* To override the default values,
      {ref}`create your own YAML configuration <creating-your-own-yaml>` to
      define the parameters of your deployment. To run the install command using
      the `example.yaml`, add the `f` parameter in you `install` command.
      Be sure to follow
      {ref}`best practices and naming conventions <kubernetes-configuration-best-practices>`
      for your configuration files.

      ```text
      helm install -f example.yaml example-trino-cluster trino/trino
      ```

   You should see output as follows:

   ```text
   NAME: example-trino-cluster
   LAST DEPLOYED: Tue Sep 13 14:12:09 2022
   NAMESPACE: default
   STATUS: deployed
   REVISION: 1
   TEST SUITE: None
   NOTES:
   Get the application URL by running these commands:
     export POD_NAME=$(kubectl get pods --namespace default -l "app=trino,release=example-trino-cluster,component=coordinator" -o jsonpath="{.items[0].metadata.name}")
     echo "Visit http://127.0.0.1:8080 to use your application"
     kubectl port-forward $POD_NAME 8080:8080
   ```

   This output depends on your configuration and cluster name. For example, the
   port `8080` is set by the `.service.port` in the `example.yaml`.

4. Run the following command to check that all pods, deployments, and services
   are running properly.

   ```text
   kubectl get all
   ```

   You should expect to see output that shows running pods, deployments, and
   replica sets. A good indicator that everything is running properly is to see
   all pods are returning a ready status in the  `READY` column.

   ```text
   NAME                                               READY   STATUS    RESTARTS   AGE
   pod/example-trino-cluster-coordinator-bfb74c98d-rnrxd   1/1     Running   0          161m
   pod/example-trino-cluster-worker-76f6bf54d6-hvl8n       1/1     Running   0          161m
   pod/example-trino-cluster-worker-76f6bf54d6-tcqgb       1/1     Running   0          161m

   NAME                       TYPE        CLUSTER-IP    EXTERNAL-IP   PORT(S)    AGE
   service/example-trino-cluster   ClusterIP   10.96.25.35   <none>        8080/TCP   161m

   NAME                                           READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/example-trino-cluster-coordinator   1/1     1            1           161m
   deployment.apps/example-trino-cluster-worker        2/2     2            2           161m

   NAME                                                     DESIRED   CURRENT   READY   AGE
   replicaset.apps/example-trino-cluster-coordinator-bfb74c98d   1         1         1       161m
   replicaset.apps/example-trino-cluster-worker-76f6bf54d6       2         2         2       161m
   ```

   The output shows running pods. These include the actual Trino containers. To
   better understand this output, check out the following resources:

   1. [kubectl get command reference](https://kubernetes.io/docs/reference/generated/kubectl/kubectl-commands#get).
   2. [kubectl get command example](https://kubernetes.io/docs/reference/kubectl/docker-cli-to-kubectl/#docker-ps).
   3. [Debugging Kubernetes reference](https://kubernetes.io/docs/tasks/debug/).

5. If all pods, deployments, and replica sets are running and in the ready
   state, Trino has been successfully deployed.

:::{note}
Unlike some Kubernetes applications, where it's better to have many small
pods, Trino works best with fewer pods each having more resources
available. We strongly recommend to avoid having multiple Trino pods on a
single physical host to avoid contention for resources.
:::

(executing-queries)=

## Executing queries

The pods running the Trino containers are all running on a private network
internal to Kubernetes. In order to access them, specifically the coordinator,
you need to create a tunnel to the coordinator pod and your computer. You can do
this by running the commands generated upon installation.

1. Store the coordinator pod name in a shell variable called `POD_NAME`.

   ```text
   POD_NAME=$(kubectl get pods -l "app=trino,release=example-trino-cluster,component=coordinator" -o name)
   ```

2. Create the tunnel from the coordinator pod to the client.

   ```text
   kubectl port-forward $POD_NAME 8080:8080
   ```

   Now you can connect to the Trino coordinator at `http://localhost:8080`.

3. To connect to Trino, you can use the
   {doc}`command-line interface </client/cli>`, a
   {doc}`JDBC client </client/jdbc>`, or any of the
   {doc}`other clients </client>`. For this example,
   {ref}`install the command-line interface <cli-installation>`, and connect to
   Trino in a new console session.

   ```text
   trino --server http://localhost:8080
   ```

4. Using the sample data in the `tpch` catalog, type and execute a query on
   the `nation` table using the `tiny` schema:

   ```text
   trino> select count(*) from tpch.tiny.nation;
    _col0
   -------
     25
   (1 row)

   Query 20181105_001601_00002_e6r6y, FINISHED, 1 node
   Splits: 21 total, 21 done (100.00%)
   0:06 [25 rows, 0B] [4 rows/s, 0B/s]
   ```

   Try other SQL queries to explore the data set and test your cluster.

5. Once you are done with your exploration, enter the `quit` command in the
   CLI.

6. Kill the tunnel to the coordinator pod. The is only available while the
   `kubectl` process is running, so you can just kill the `kubectl` process
   that's forwarding the port. In most cases that means pressing `CTRL` +
   `C` in the terminal where the port-forward command is running.

## Configuration

The Helm chart uses the {doc}`Trino container image </installation/containers>`.
The Docker image already contains a default configuration to get started, and
some catalogs to allow you to explore Trino. Kubernetes allows you to mimic a
{doc}`traditional deployment </installation/deployment>` by supplying
configuration in YAML files. It's important to understand how files such as the
Trino configuration, JVM, and various {doc}`catalog properties </connector>` are
configured in Trino before updating the values.

(creating-your-own-yaml)=

### Creating your own YAML configuration

When you use your own YAML Kubernetes configuration, you only override the values you specify.
The remaining properties use their default values. Add an `example.yaml` with
the following configuration:

```yaml
image:
  tag: "|trino_version|"
server:
  workers: 3
coordinator:
  jvm:
    maxHeapSize: "8G"
worker:
  jvm:
    maxHeapSize: "8G"
```

These values are higher than the defaults and allow Trino to use more memory
and run more demanding queries. If the values are too high, Kubernetes might
not be able to schedule some Trino pods, depending on other applications
deployed in this cluster and the size of the cluster nodes.

1. `.image.tag` is set to the current version, |trino_version|. Set
   this value if you need to use a specific version of Trino. The default is
   `latest`, which is not recommended. Using `latest` will publish a new
   version of Trino with each release and a following Kubernetes deployment.
2. `.server.workers` is set to `3`. This value sets the number of
   workers, in this case, a coordinator and three worker nodes are deployed.
3. `.coordinator.jvm.maxHeapSize` is set to `8GB`.
   This sets the maximum heap size in the JVM of the coordinator. See
   {ref}`jvm-config`.
4. `.worker.jvm.maxHeapSize` is set to `8GB`.
   This sets the maximum heap size in the JVM of the worker. See
   {ref}`jvm-config`.

:::{warning}
Some memory settings need to be tuned carefully as setting some values
outside of the range of the maximum heap size will cause Trino startup to
fail. See the warnings listed on {doc}`/admin/properties-resource-management`.
:::

Reference [the full list of properties](https://trinodb.github.io/charts/charts/trino/)
that can be overridden in the Helm chart.

(kubernetes-configuration-best-practices)=

:::{note}
Although `example.yaml` is used to refer to the Kubernetes configuration
file in this document, you should use clear naming guidelines for the cluster
and deployment you are managing. For example,
`cluster-example-trino-etl.yaml` might refer to a Trino deployment for a
cluster used primarily for extract-transform-load queries deployed on the
`example` Kubernetes cluster. See
[Configuration Best Practices](https://kubernetes.io/docs/concepts/configuration/overview/)
for more tips on configuring Kubernetes deployments.
:::

### Adding catalogs

A common use-case is to add custom catalogs. You can do this by adding values to
the `additionalCatalogs` property in the `example.yaml` file.

```yaml
additionalCatalogs:
  lakehouse: |-
    connector.name=iceberg
    hive.metastore.uri=thrift://example.net:9083
  rdbms: |-
    connector.name=postgresql
    connection-url=jdbc:postgresql://example.net:5432/database
    connection-user=root
    connection-password=secret
```

This adds both `lakehouse` and `rdbms` catalogs to the Kubernetes deployment
configuration.

(running-a-local-kubernetes-cluster-with-kind)=

## Running a local Kubernetes cluster with kind

For local deployments, you can use
[kind (Kubernetes in Docker)](https://kind.sigs.k8s.io). Follow the steps
below to run `kind` on your system.

1. `kind` runs on [Docker](https://www.docker.com), so first check if Docker
   is installed:

   ```text
   docker --version
   ```

   If this command fails, install Docker by following
   [Docker installation instructions](https://docs.docker.com/engine/install/).

2. Install `kind` by following the
   [kind installation instructions](https://kind.sigs.k8s.io/docs/user/quick-start/#installation).

3. Run a Kubernetes cluster in `kind` by running the command:

   ```text
   kind create cluster --name trino
   ```

   :::{note}
   The `name` parameter is optional but is used to showcase how the
   namespace is applied in future commands. The cluster name defaults to
   `kind` if no parameter is added. Use `trino` to make the application
   on this cluster obvious.
   :::

4. Verify that `kubectl` is running against the correct Kubernetes cluster.

   ```text
   kubectl cluster-info --context kind-trino
   ```

   If you have multiple Kubernetes clusters already configured within
   `~/.kube/config`, you need to pass the `context` parameter to the
   `kubectl` commands to operate with the local `kind` cluster. `kubectl`
   uses the
   [default context](https://kubernetes.io/docs/reference/kubectl/cheatsheet/#kubectl-context-and-configuration)
   if this parameter isn't supplied. Notice the context is the name of the
   cluster with the `kind-` prefix added. Now you can look at all the
   Kubernetes objects running on your `kind` cluster.

5. Set up Trino by folling the {ref}`running-trino-using-helm` steps. When
   running the `kubectl get all` command, add the `context` parameter.

   ```text
   kubectl get all --context kind-trino
   ```

6. Run some queries by following the [Executing queries](#executing-queries) steps.

7. Once you are done with the cluster using kind, you can delete the cluster.

   ```text
   kind delete cluster -n trino
   ```

## Cleaning up

To uninstall Trino from the Kubernetes cluster, run the following command:

```text
helm uninstall my-trino-cluster
```

You should expect to see the following output:

```text
release "my-trino-cluster" uninstalled
```

To validate that this worked, you can run this `kubectl` command to make sure
there are no remaining Kubernetes objects related to the Trino cluster.

```text
kubectl get all
```
