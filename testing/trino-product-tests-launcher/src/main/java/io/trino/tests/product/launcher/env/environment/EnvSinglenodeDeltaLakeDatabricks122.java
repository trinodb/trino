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
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;

@TestsEnvironment
public class EnvSinglenodeDeltaLakeDatabricks122
        extends AbstractSinglenodeDeltaLakeDatabricks
{
    @Inject
    public EnvSinglenodeDeltaLakeDatabricks122(Standard standard, DockerFiles dockerFiles)
    {
        super(standard, dockerFiles);
    }

    @Override
    String databricksTestJdbcUrl()
    {
        return requireEnv("DATABRICKS_122_JDBC_URL");
    }
}
