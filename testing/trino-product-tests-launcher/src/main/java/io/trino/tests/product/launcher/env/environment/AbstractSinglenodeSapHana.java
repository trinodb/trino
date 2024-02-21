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

import com.starburstdata.presto.testing.testcontainers.SapHanaDockerInitializer;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import java.io.File;

import static io.trino.testing.TestingProperties.getSapHanaJdbcDriverVersion;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forClasspathResource;
import static org.testcontainers.utility.MountableFile.forHostPath;

public abstract class AbstractSinglenodeSapHana
        extends EnvironmentProvider
{
    public static final File JDBC_DRIVER = new File("testing/trino-product-tests-launcher/target/ngdbc-%s.jar".formatted(getSapHanaJdbcDriverVersion()));
    protected static final String CONTAINER_NAME = "sap-hana";

    private final SapHanaDockerInitializer dockerInitializer;
    protected final DockerFiles dockerFiles;

    protected AbstractSinglenodeSapHana(Standard standard, PortBinder portBinder, DockerFiles dockerFiles)
    {
        super(standard);
        dockerInitializer = new SapHanaDockerInitializer((genericContainer, port) -> portBinder.exposePort((DockerContainer) genericContainer, port));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        DockerContainer dockerContainer = new DockerContainer(SapHanaDockerInitializer.SAP_HANA_DOCKER_IMAGE, CONTAINER_NAME);
        dockerInitializer.apply(dockerContainer);
        builder.addContainer(dockerContainer);

        builder.configureContainer(COORDINATOR, container -> container
                .withCopyFileToContainer(forHostPath(JDBC_DRIVER.getAbsolutePath()), "/docker/sap-hana-jdbc-driver/ngdbc.jar")
                .withCopyFileToContainer(forClasspathResource("install-sap-hana-jdbc-driver.sh", 0755), "/docker/presto-init.d/install-sap-hana-jdbc-driver.sh"));
    }
}
