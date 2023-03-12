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

import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Standard;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * Trino with Delta Lake connector and real S3 storage
 */
public abstract class AbstractSinglenodeDeltaLakeDatabricks
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;

    abstract String databricksTestJdbcUrl();

    public AbstractSinglenodeDeltaLakeDatabricks(Standard standard, DockerFiles dockerFiles)
    {
        super(standard);
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String databricksTestJdbcUrl = databricksTestJdbcUrl();
        String databricksTestLogin = requireNonNull(System.getenv("DATABRICKS_LOGIN"), "Environment DATABRICKS_LOGIN was not set");
        String databricksTestToken = requireNonNull(System.getenv("DATABRICKS_TOKEN"), "Environment DATABRICKS_TOKEN was not set");
        String awsRegion = requireNonNull(System.getenv("AWS_REGION"), "Environment AWS_REGION was not set");
        String s3Bucket = requireNonNull(System.getenv("S3_BUCKET"), "Environment S3_BUCKET was not set");
        DockerFiles.ResourceProvider configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/singlenode-delta-lake-databricks");

        builder.configureContainer(COORDINATOR, dockerContainer -> exportAWSCredentials(dockerContainer)
                .withEnv("AWS_REGION", awsRegion)
                .withEnv("DATABRICKS_JDBC_URL", databricksTestJdbcUrl)
                .withEnv("DATABRICKS_LOGIN", databricksTestLogin)
                .withEnv("DATABRICKS_TOKEN", databricksTestToken));
        builder.addConnector("hive", forHostPath(configDir.getPath("hive.properties")));
        builder.addConnector(
                "delta_lake",
                forHostPath(configDir.getPath("delta.properties")),
                CONTAINER_TRINO_ETC + "/catalog/delta.properties");

        builder.configureContainer(TESTS, container -> exportAWSCredentials(container)
                .withEnv("S3_BUCKET", s3Bucket)
                .withEnv("AWS_REGION", awsRegion)
                .withEnv("DATABRICKS_JDBC_URL", databricksTestJdbcUrl)
                .withEnv("DATABRICKS_LOGIN", databricksTestLogin)
                .withEnv("DATABRICKS_TOKEN", databricksTestToken));

        configureTempto(builder, configDir);
    }

    private DockerContainer exportAWSCredentials(DockerContainer container)
    {
        container = exportAWSCredential(container, "TRINO_AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID", true);
        container = exportAWSCredential(container, "TRINO_AWS_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY", true);
        return exportAWSCredential(container, "TRINO_AWS_SESSION_TOKEN", "AWS_SESSION_TOKEN", false);
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
