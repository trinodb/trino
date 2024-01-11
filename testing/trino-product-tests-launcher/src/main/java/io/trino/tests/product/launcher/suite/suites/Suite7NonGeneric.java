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
import io.trino.tests.product.launcher.env.environment.EnvMultinodeKerberosKudu;
import io.trino.tests.product.launcher.env.environment.EnvMultinodePostgresql;
import io.trino.tests.product.launcher.env.environment.EnvMultinodeSqlserver;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeKerberosHdfsImpersonationCrossRealm;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeSparkHive;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeSparkHiveNoStatsFallback;
import io.trino.tests.product.launcher.env.environment.EnvTwoKerberosHives;
import io.trino.tests.product.launcher.env.environment.EnvTwoMixedHives;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.tests.product.TestGroups.CLI;
import static io.trino.tests.product.TestGroups.CONFIGURED_FEATURES;
import static io.trino.tests.product.TestGroups.HDFS_IMPERSONATION;
import static io.trino.tests.product.TestGroups.HIVE_KERBEROS;
import static io.trino.tests.product.TestGroups.HIVE_SPARK;
import static io.trino.tests.product.TestGroups.HIVE_SPARK_NO_STATS_FALLBACK;
import static io.trino.tests.product.TestGroups.KUDU;
import static io.trino.tests.product.TestGroups.POSTGRESQL;
import static io.trino.tests.product.TestGroups.SQLSERVER;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.TestGroups.TWO_HIVES;
import static io.trino.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class Suite7NonGeneric
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        verify(config.getHadoopBaseImage().equals(EnvironmentDefaults.HADOOP_BASE_IMAGE), "The suite should be run with default HADOOP_BASE_IMAGE. Leave HADOOP_BASE_IMAGE unset.");

        return ImmutableList.of(
                testOnEnvironment(EnvMultinodePostgresql.class)
                        .withGroups(CONFIGURED_FEATURES, POSTGRESQL)
                        .build(),
                testOnEnvironment(EnvMultinodeSqlserver.class)
                        .withGroups(CONFIGURED_FEATURES, SQLSERVER)
                        .build(),
                testOnEnvironment(EnvSinglenodeSparkHive.class)
                        .withGroups(CONFIGURED_FEATURES, HIVE_SPARK)
                        .build(),
                testOnEnvironment(EnvSinglenodeSparkHiveNoStatsFallback.class)
                        .withGroups(CONFIGURED_FEATURES, HIVE_SPARK_NO_STATS_FALLBACK)
                        .build(),
                testOnEnvironment(EnvSinglenodeKerberosHdfsImpersonationCrossRealm.class)
                        .withGroups(CONFIGURED_FEATURES, STORAGE_FORMATS, CLI, HDFS_IMPERSONATION, HIVE_KERBEROS)
                        .build(),
                testOnEnvironment(EnvMultinodeKerberosKudu.class)
                        .withGroups(CONFIGURED_FEATURES, KUDU)
                        .build(),
                testOnEnvironment(EnvTwoMixedHives.class)
                        .withGroups(CONFIGURED_FEATURES, TWO_HIVES)
                        .build(),
                testOnEnvironment(EnvTwoKerberosHives.class)
                        .withGroups(CONFIGURED_FEATURES, TWO_HIVES)
                        .build());
    }
}
