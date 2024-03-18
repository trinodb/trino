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

import io.airlift.log.Logger;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.common.EnvironmentExtender;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static org.testcontainers.containers.BindMode.READ_ONLY;

public abstract class MultinodeWarpGlueBase
        extends MultinodeWarpBase
{
    private static final Logger logger = Logger.get(MultinodeWarpGlueBase.class);

    public MultinodeWarpGlueBase(String dockerFilesHostDirectory,
            DockerFiles dockerFiles,
            PortBinder portBinder,
            EnvironmentExtender... bases)
    {
        super(dockerFilesHostDirectory, dockerFiles, portBinder, bases);
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        super.extendEnvironment(builder);

        builder.configureContainer(COORDINATOR, MultinodeWarpGlueBase::configureEnv);
        builder.configureContainer(WORKER, MultinodeWarpGlueBase::configureEnv);
    }

    public static void configureEnv(DockerContainer container)
    {
        String profile = System.getenv("PT_AWS_PROFILE");

        if (profile != null) {
            logger.info("Using AWS credentials from AWS_PROFILE %s", profile);
            container.withFileSystemBind(System.getProperty("user.home") + "/.aws", "/root/.aws", READ_ONLY)
                    .withEnv("AWS_PROFILE", profile);
        }
        else {
            logger.info("Using AWS credentials from environment");
            container.withEnv("AWS_ACCESS_KEY_ID", requireEnv("AWS_ACCESS_KEY_ID"))
                    .withEnv("AWS_REGION", "us-east-1")
                    .withEnv("AWS_SECRET_ACCESS_KEY", requireEnv("AWS_SECRET_ACCESS_KEY"));
            if (System.getenv("AWS_SESSION_TOKEN") != null) {
                container.withEnv("AWS_SESSION_TOKEN", requireEnv("AWS_SESSION_TOKEN"));
            }
        }
    }
}
