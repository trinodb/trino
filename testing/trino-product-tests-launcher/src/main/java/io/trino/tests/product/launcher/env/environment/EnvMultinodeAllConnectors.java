/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.product.launcher.env.environment;

import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import java.util.List;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.isTrinoContainer;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_JVM_CONFIG;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeAllConnectors
        extends EnvironmentProvider
{
    private final ResourceProvider configDir;

    @Inject
    public EnvMultinodeAllConnectors(StandardMultinode standardMultinode, DockerFiles dockerFiles)
    {
        super(standardMultinode);
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-all/");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        // blackhole, jmx, tpch are already configured in Standard base env
        List.of(
                        // TODO accumulo needs to connect to ZooKeeper, it won't start otherwise
                        //"accumulo",
                        "atop",
                        "bigquery",
                        "cassandra",
                        "clickhouse",
                        "druid",
                        "delta_lake",
                        "elasticsearch",
                        "gsheets",
                        "hive",
                        "hudi",
                        "iceberg",
                        "ignite",
                        "kafka",
                        "kinesis",
                        "kudu",
                        "localfile",
                        "mariadb",
                        "memory",
                        "singlestore",
                        "mongodb",
                        "mysql",
                        "oracle",
                        "phoenix5",
                        "pinot",
                        "postgresql",
                        "prometheus",
                        "raptor_legacy",
                        "redis",
                        "redshift",
                        "sqlserver",
                        "trino_thrift",
                        "tpcds")
                .forEach(connector -> builder.addConnector(
                        connector,
                        forHostPath(configDir.getPath(connector + ".properties"))));
        builder.configureContainers(container -> {
            if (isTrinoContainer(container.getLogicalName())) {
                container.withCopyFileToContainer(
                        forHostPath(configDir.getPath("google-sheets-auth.json")),
                        CONTAINER_TRINO_ETC + "/catalog/google-sheets-auth.json");
                container.withCopyFileToContainer(
                        forHostPath(configDir.getPath("prometheus-bearer.txt")),
                        CONTAINER_TRINO_ETC + "/catalog/prometheus-bearer.txt");
                container.withCopyFileToContainer(
                        forHostPath(configDir.getPath("jvm.config")),
                        CONTAINER_TRINO_JVM_CONFIG);
            }
        });
    }
}
