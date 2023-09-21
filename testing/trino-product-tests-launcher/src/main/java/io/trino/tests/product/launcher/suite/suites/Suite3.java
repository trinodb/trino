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
import io.trino.tests.product.launcher.env.environment.EnvMultinodeTls;
import io.trino.tests.product.launcher.env.environment.EnvMultinodeTlsKerberos;
import io.trino.tests.product.launcher.env.environment.EnvMultinodeTlsKerberosDelegation;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeKerberosHdfsImpersonationWithDataProtection;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeKerberosHdfsImpersonationWithWireEncryption;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static io.trino.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class Suite3
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        return ImmutableList.of(
                testOnEnvironment(EnvMultinodeTls.class)
                        .withGroups("configured_features", "smoke", "cli", "group-by", "join", "tls")
                        .withExcludedGroups("azure")
                        .build(),
                testOnEnvironment(EnvMultinodeTlsKerberos.class)
                        .withGroups("configured_features", "cli", "group-by", "join", "tls")
                        .build(),
                testOnEnvironment(EnvSinglenodeKerberosHdfsImpersonationWithWireEncryption.class)
                        .withGroups("configured_features", "storage_formats", "cli", "hdfs_impersonation", "authorization")
                        .build(),
                testOnEnvironment(EnvSinglenodeKerberosHdfsImpersonationWithDataProtection.class)
                        .withGroups("configured_features")
                        .withTests("TestHiveStorageFormats.testOrcTableCreatedInTrino", "TestHiveCreateTable.testCreateTable")
                        .build(),
                testOnEnvironment(EnvMultinodeTlsKerberosDelegation.class)
                        .withGroups("configured_features", "jdbc", "jdbc_kerberos_constrained_delegation")
                        .build());
    }
}
