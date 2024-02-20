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
import io.trino.tests.product.launcher.env.environment.EnvMultinodeIcebergMinioCaching;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeHiveIcebergRedirections;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeSparkIceberg;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeSparkIcebergJdbcCatalog;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeSparkIcebergNessie;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeSparkIcebergRest;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.tests.product.TestGroups.CONFIGURED_FEATURES;
import static io.trino.tests.product.TestGroups.HIVE_ICEBERG_REDIRECTIONS;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.ICEBERG_ALLUXIO_CACHING;
import static io.trino.tests.product.TestGroups.ICEBERG_JDBC;
import static io.trino.tests.product.TestGroups.ICEBERG_NESSIE;
import static io.trino.tests.product.TestGroups.ICEBERG_REST;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
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
                        .withGroups(CONFIGURED_FEATURES, ICEBERG)
                        .withExcludedGroups(STORAGE_FORMATS)
                        .build(),
                testOnEnvironment(EnvSinglenodeHiveIcebergRedirections.class)
                        .withGroups(CONFIGURED_FEATURES, HIVE_ICEBERG_REDIRECTIONS)
                        .build(),
                testOnEnvironment(EnvSinglenodeSparkIcebergRest.class)
                        .withGroups(CONFIGURED_FEATURES, ICEBERG_REST)
                        .build(),
                testOnEnvironment(EnvSinglenodeSparkIcebergJdbcCatalog.class)
                        .withGroups(CONFIGURED_FEATURES, ICEBERG_JDBC)
                        .build(),
                testOnEnvironment(EnvSinglenodeSparkIcebergNessie.class)
                        .withGroups(CONFIGURED_FEATURES, ICEBERG_NESSIE)
                        .build(),
                testOnEnvironment(EnvMultinodeIcebergMinioCaching.class)
                        .withGroups(CONFIGURED_FEATURES, ICEBERG_ALLUXIO_CACHING)
                        .withExcludedGroups(STORAGE_FORMATS)
                        .build());
    }
}
