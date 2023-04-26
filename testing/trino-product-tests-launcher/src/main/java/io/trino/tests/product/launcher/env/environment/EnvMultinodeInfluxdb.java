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
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import java.time.Duration;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.wait.strategy.Wait.forHttp;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeInfluxdb
        extends EnvironmentProvider
{
    private static final String INFLUXDB = "influxdb";
    private static final int INFLUXDB_PORT = 8086;
    public static final String USERNAME = "admin";
    public static final String PASSWORD = "password";

    private final DockerFiles.ResourceProvider configDir;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodeInfluxdb(StandardMultinode standardMultinode, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(standardMultinode);
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-influxdb/");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addContainer(createInfluxDb());
        builder.addConnector(INFLUXDB, forHostPath(configDir.getPath("influxdb.properties")));
        configureTempto(builder, configDir);
    }

    private DockerContainer createInfluxDb()
    {
        DockerContainer container = new DockerContainer("influxdb:1.8.10", "influxdb")
                .withEnv("INFLUXDB_DB", "presto")
                .withEnv("INFLUXDB_ADMIN_USER", USERNAME)
                .withEnv("INFLUXDB_ADMIN_PASSWORD", PASSWORD)
                .withEnv("INFLUXDB_USER", "test")
                .withEnv("INFLUXDB_USER_PASSWORD", "password")
                .withEnv("INFLUXDB_INIT_DATAFILE", "init-influxdb.data")
                .withExposedPorts(INFLUXDB_PORT)
                .withCopyFileToContainer(forHostPath(configDir.getPath("init-influxdb.sh")), "/init-influxdb.sh")
                .withCopyFileToContainer(forHostPath(configDir.getPath("init-influxdb.data")), "/init-influxdb.data")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forHttp("/health").forPort(INFLUXDB_PORT).forStatusCode(200))
                .withStartupTimeout(Duration.ofMinutes(1));

        portBinder.exposePort(container, INFLUXDB_PORT);

        return container;
    }
}
