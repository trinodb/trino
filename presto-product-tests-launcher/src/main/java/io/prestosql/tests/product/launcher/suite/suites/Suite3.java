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
package io.prestosql.tests.product.launcher.suite.suites;

import com.google.common.collect.ImmutableList;
import io.prestosql.tests.product.launcher.env.EnvironmentConfig;
import io.prestosql.tests.product.launcher.env.environment.MultinodeTls;
import io.prestosql.tests.product.launcher.env.environment.MultinodeTlsKerberos;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeKerberosHdfsImpersonationWithDataProtection;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeKerberosHdfsImpersonationWithWireEncryption;
import io.prestosql.tests.product.launcher.suite.Suite;
import io.prestosql.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static io.prestosql.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class Suite3
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        return ImmutableList.of(
                testOnEnvironment(MultinodeTls.class)
                        .withGroups("smoke", "cli", "group-by", "join", "tls")
                        .build(),
                testOnEnvironment(MultinodeTlsKerberos.class)
                        .withGroups("cli", "group-by", "join", "tls")
                        .build(),
                testOnEnvironment(SinglenodeKerberosHdfsImpersonationWithWireEncryption.class)
                        .withGroups("storage_formats", "cli", "hdfs_impersonation,authorization")
                        .build(),
                testOnEnvironment(SinglenodeKerberosHdfsImpersonationWithDataProtection.class)
                        .withTests("TestHiveStorageFormats.testOrcTableCreatedInPresto", "TestHiveCreateTable.testCreateTable")
                        .build());
    }
}
