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

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeDatabricksHttpHms
        extends EnvironmentProvider
{
    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public EnvMultinodeDatabricksHttpHms(StandardMultinode standardMultinode, DockerFiles dockerFiles)
    {
        super(standardMultinode);
        requireNonNull(dockerFiles, "dockerFiles is null");
        configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-databricks-http-hms");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String databricksTestJdbcUrl = requireNonNull(System.getenv("DATABRICKS_UNITY_JDBC_URL"), "Environment DATABRICKS_UNITY_JDBC_URL was not set");
        String databricksTestLogin = requireNonNull(System.getenv("DATABRICKS_LOGIN"), "Environment DATABRICKS_LOGIN was not set");
        String databricksTestToken = requireNonNull(System.getenv("DATABRICKS_TOKEN"), "Environment DATABRICKS_TOKEN was not set");
        String awsRegion = requireNonNull(System.getenv("AWS_REGION"), "Environment AWS_REGION was not set");

        builder.configureContainer(COORDINATOR, dockerContainer -> exportAWSCredentials(dockerContainer)
                .withEnv("AWS_REGION", awsRegion)
                .withEnv("DATABRICKS_TOKEN", databricksTestToken)
                .withEnv("DATABRICKS_HOST", getEnvVariable("DATABRICKS_HOST"))
                .withEnv("DATABRICKS_UNITY_CATALOG_NAME", getEnvVariable("DATABRICKS_UNITY_CATALOG_NAME")));

        builder.configureContainer(WORKER, dockerContainer -> exportAWSCredentials(dockerContainer)
                .withEnv("AWS_REGION", awsRegion)
                .withEnv("DATABRICKS_TOKEN", databricksTestToken)
                .withEnv("DATABRICKS_HOST", getEnvVariable("DATABRICKS_HOST"))
                .withEnv("DATABRICKS_UNITY_CATALOG_NAME", getEnvVariable("DATABRICKS_UNITY_CATALOG_NAME")));

        builder.configureContainer(TESTS, container -> exportAWSCredentials(container)
                .withEnv("DATABRICKS_JDBC_URL", databricksTestJdbcUrl)
                .withEnv("DATABRICKS_LOGIN", databricksTestLogin)
                .withEnv("DATABRICKS_TOKEN", databricksTestToken)
                .withEnv("DATABRICKS_UNITY_CATALOG_NAME", getEnvVariable("DATABRICKS_UNITY_CATALOG_NAME"))
                .withEnv("DATABRICKS_UNITY_EXTERNAL_LOCATION", getEnvVariable("DATABRICKS_UNITY_EXTERNAL_LOCATION")));

        builder.addConnector("hive", forHostPath(configDir.getPath("hive.properties")));
        builder.addConnector(
                "delta_lake",
                forHostPath(configDir.getPath("delta.properties")),
                "/docker/presto-product-tests/conf/presto/etc/catalog/delta.properties");
        configureTempto(builder, configDir);
    }

    private DockerContainer exportAWSCredentials(DockerContainer container)
    {
        container = exportAWSCredential(container, "TRINO_AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID", true);
        container = exportAWSCredential(container, "TRINO_AWS_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY", true);
        return exportAWSCredential(container, "TRINO_AWS_SESSION_TOKEN", "AWS_SESSION_TOKEN", false);
    }

    private String getEnvVariable(String name)
    {
        String credentialValue = System.getenv(name);
        if (credentialValue == null) {
            throw new IllegalStateException(format("Environment variable %s not set", name));
        }
        return credentialValue;
    }

    private DockerContainer exportAWSCredential(DockerContainer container, String credentialEnvVariable, String containerEnvVariable, boolean required)
    {
        String credentialValue = System.getenv(credentialEnvVariable);
        if (credentialValue == null) {
            if (required) {
                throw new IllegalStateException(format("Environment variable %s not set", credentialEnvVariable));
            }
            return container;
        }
        return container.withEnv(containerEnvVariable, credentialValue);
    }
}
