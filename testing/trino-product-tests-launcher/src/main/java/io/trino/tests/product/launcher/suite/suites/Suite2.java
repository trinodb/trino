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
import io.trino.tests.product.launcher.env.environment.EnvMultinode;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeHdfsImpersonation;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeKerberosHdfsImpersonation;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeKerberosHdfsNoImpersonation;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeKerberosHiveNoImpersonationWithCredentialCache;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static io.trino.tests.product.TestGroups.AUTHORIZATION;
import static io.trino.tests.product.TestGroups.CLI;
import static io.trino.tests.product.TestGroups.CONFIGURED_FEATURES;
import static io.trino.tests.product.TestGroups.HDFS_IMPERSONATION;
import static io.trino.tests.product.TestGroups.HDFS_NO_IMPERSONATION;
import static io.trino.tests.product.TestGroups.HIVE_FILE_HEADER;
import static io.trino.tests.product.TestGroups.HIVE_KERBEROS;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class Suite2
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        return ImmutableList.of(
                testOnEnvironment(EnvMultinode.class)
                        .withGroups(CONFIGURED_FEATURES, HDFS_NO_IMPERSONATION)
                        // hive.non-managed-table-writes-enabled is mandatory for this test,
                        // setting up this property will break other tests
                        .withExcludedTests("io.trino.tests.product.TestImpersonation.testExternalLocationTableCreationSuccess")
                        .build(),
                testOnEnvironment(EnvSinglenodeKerberosHdfsNoImpersonation.class)
                        .withGroups(CONFIGURED_FEATURES, STORAGE_FORMATS, HDFS_NO_IMPERSONATION, HIVE_KERBEROS)
                        .build(),
                testOnEnvironment(EnvSinglenodeKerberosHiveNoImpersonationWithCredentialCache.class)
                        .withGroups(CONFIGURED_FEATURES, STORAGE_FORMATS, HDFS_NO_IMPERSONATION)
                        .build(),
                testOnEnvironment(EnvSinglenodeHdfsImpersonation.class)
                        .withGroups(CONFIGURED_FEATURES, STORAGE_FORMATS, CLI, HDFS_IMPERSONATION)
                        .build(),
                testOnEnvironment(EnvSinglenodeKerberosHdfsImpersonation.class)
                        .withGroups(CONFIGURED_FEATURES, STORAGE_FORMATS, CLI, HDFS_IMPERSONATION, AUTHORIZATION, HIVE_FILE_HEADER, HIVE_KERBEROS)
                        .build());
    }
}
