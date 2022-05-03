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

import com.google.common.collect.ImmutableList;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import java.io.File;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_HADOOP_INIT_D;
import static org.testcontainers.utility.MountableFile.forClasspathResource;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvSinglenodeOpenX
        extends EnvironmentProvider
{
    public static final File OPENX_SERDE = new File("testing/trino-product-tests-launcher/target/json-serde-1.3.9-e.8-jar-with-dependencies.jar");

    @Inject
    protected EnvSinglenodeOpenX(Standard standard, Hadoop hadoop)
    {
        super(ImmutableList.of(standard, hadoop));
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(HADOOP, dockerContainer -> {
            dockerContainer.withCopyFileToContainer(
                    forHostPath(OPENX_SERDE.getAbsolutePath()),
                    "/docker/json-serde-jar-with-dependencies.jar");
            dockerContainer.withCopyFileToContainer(
                    forClasspathResource("install-openx-serde-in-hive.sh", 0755),
                    CONTAINER_HADOOP_INIT_D + "install-openx-serde.sh");
        });
    }
}
