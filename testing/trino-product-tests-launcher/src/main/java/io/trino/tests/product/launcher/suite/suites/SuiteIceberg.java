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
package io.trino.tests.product.launcher.suite.suites;

import com.google.common.collect.ImmutableList;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentDefaults;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeHiveIcebergRedirections;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeSparkIceberg;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeSparkIcebergJdbcCatalog;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeSparkIcebergNessie;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeSparkIcebergRest;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class SuiteIceberg
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        verify(config.getHadoopBaseImage().equals(EnvironmentDefaults.HADOOP_BASE_IMAGE), "The suite should be run with default HADOOP_BASE_IMAGE. Leave HADOOP_BASE_IMAGE unset.");

        return ImmutableList.of(
                testOnEnvironment(EnvSinglenodeSparkIceberg.class)
                        .withGroups("configured_features", "iceberg")
                        .withExcludedGroups("storage_formats")
                        .build(),
                testOnEnvironment(EnvSinglenodeHiveIcebergRedirections.class)
                        .withGroups("configured_features", "hive_iceberg_redirections")
                        .build(),
                testOnEnvironment(EnvSinglenodeSparkIcebergRest.class)
                        .withGroups("configured_features", "iceberg_rest")
                        .build(),
                testOnEnvironment(EnvSinglenodeSparkIcebergJdbcCatalog.class)
                        .withGroups("configured_features", "iceberg_jdbc")
                        .build(),
                testOnEnvironment(EnvSinglenodeSparkIcebergNessie.class)
                        .withGroups("configured_features", "iceberg_nessie")
                        .build());
    }
}
