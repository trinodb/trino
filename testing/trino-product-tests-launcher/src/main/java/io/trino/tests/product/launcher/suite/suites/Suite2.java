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

import static io.trino.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class Suite2
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        return ImmutableList.of(
                testOnEnvironment(EnvMultinode.class)
                        .withGroups("configured_features", "hdfs_no_impersonation")
                        .withExcludedTests("io.trino.tests.product.TestImpersonation.testExternalLocationTableCreationSuccess")
                        .build(),
                testOnEnvironment(EnvSinglenodeKerberosHdfsNoImpersonation.class)
                        .withGroups("configured_features", "storage_formats", "hdfs_no_impersonation", "hive_kerberos")
                        .build(),
                testOnEnvironment(EnvSinglenodeKerberosHiveNoImpersonationWithCredentialCache.class)
                        .withGroups("configured_features", "storage_formats", "hdfs_no_impersonation")
                        .build(),
                testOnEnvironment(EnvSinglenodeHdfsImpersonation.class)
                        .withGroups("configured_features", "storage_formats", "cli", "hdfs_impersonation")
                        .build(),
                testOnEnvironment(EnvSinglenodeKerberosHdfsImpersonation.class)
                        .withGroups("configured_features", "storage_formats", "cli", "hdfs_impersonation", "authorization", "hive_file_header", "hive_kerberos")
                        .build());
    }
}
