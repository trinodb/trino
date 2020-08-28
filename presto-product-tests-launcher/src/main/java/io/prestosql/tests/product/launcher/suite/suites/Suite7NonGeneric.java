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
import io.prestosql.tests.product.launcher.env.EnvironmentDefaults;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeKerberosHdfsImpersonationCrossRealm;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeLdapBindDn;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeMysql;
import io.prestosql.tests.product.launcher.env.environment.SinglenodePostgresql;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeSparkIceberg;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeSqlserver;
import io.prestosql.tests.product.launcher.env.environment.TwoKerberosHives;
import io.prestosql.tests.product.launcher.env.environment.TwoMixedHives;
import io.prestosql.tests.product.launcher.suite.Suite;
import io.prestosql.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.prestosql.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class Suite7NonGeneric
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        verify(config.getHadoopBaseImage().equals(EnvironmentDefaults.HADOOP_BASE_IMAGE), "The suite should be run with default HADOOP_BASE_IMAGE. Leave HADOOP_BASE_IMAGE unset.");

        return ImmutableList.of(
                /**
                 * Does not use hadoop
                 *
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment singlenode-mysql \
                 *     -- -g mysql \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(SinglenodeMysql.class).withGroups("mysql").build(),

                /**
                 * Does not use hadoop
                 *
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment singlenode-postgresql \
                 *     -- -g postgresql \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(SinglenodePostgresql.class).withGroups("postgresql").build(),

                /**
                 * Does not use hadoop
                 *
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment singlenode-sqlserver \
                 *     -- -g sqlserver \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(SinglenodeSqlserver.class).withGroups("sqlserver").build(),

                /**
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment singlenode-spark-iceberg \
                 *     -- -g iceberg -x storage_formats \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(SinglenodeSparkIceberg.class).withGroups("iceberg").withExcludedGroups("storage_formats").build(),

                /**
                 * Environment not set up on CDH. (TODO run on HDP 2.6 and HDP 3.1)
                 *
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment singlenode-kerberos-hdfs-impersonation-cross-realm \
                 *     -- -g storage_formats,cli,hdfs_impersonation \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(SinglenodeKerberosHdfsImpersonationCrossRealm.class).withGroups("storage_formats", "cli", "hdfs_impersonation").build(),

                /**
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment two-mixed-hives \
                 *     -- -g two_hives \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(TwoMixedHives.class).withGroups("two_hives").build(),

                /**
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment two-kerberos-hives \
                 *     -- -g two_hives \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(TwoKerberosHives.class).withGroups("two_hives").build(),

                /**
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment singlenode-ldap-bind-dn \
                 *     -- -g ldap \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(SinglenodeLdapBindDn.class).withGroups("ldap").build());
    }
}
