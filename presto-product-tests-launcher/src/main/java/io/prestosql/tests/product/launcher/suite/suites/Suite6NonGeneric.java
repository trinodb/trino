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
import io.prestosql.tests.product.launcher.env.environment.SinglenodeCassandra;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeKafka;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeKerberosKmsHdfsImpersonation;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeKerberosKmsHdfsNoImpersonation;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeLdap;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeLdapInsecure;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeLdapReferrals;
import io.prestosql.tests.product.launcher.suite.Suite;
import io.prestosql.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.prestosql.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class Suite6NonGeneric
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        verify(config.getHadoopBaseImage().equals(EnvironmentDefaults.HADOOP_BASE_IMAGE), "The suite should be run with default HADOOP_BASE_IMAGE. Leave HADOOP_BASE_IMAGE unset.");

        return ImmutableList.of(
                /**
                 * Uses hadoop, but it irrelevant from these tests' perspective.
                 *
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment singlenode-ldap \
                 *     -- -g ldap \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(SinglenodeLdap.class).withGroups("ldap").build(),

                /**
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment singlenode-ldap-insecure \
                 *     -- -g ldap \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(SinglenodeLdapInsecure.class).withGroups("ldap").build(),

                /**
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment singlenode-ldap-referrals \
                 *     -- -g ldap \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(SinglenodeLdapReferrals.class).withGroups("ldap").build(),

                /**
                 * We have docker images with KMS on CDH only. TODO (https://github.com/prestosql/presto/issues/1652) create images with HDP and KMS
                 *
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment singlenode-kerberos-kms-hdfs-no-impersonation \
                 *     -- -g storage_formats \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(SinglenodeKerberosKmsHdfsNoImpersonation.class).withGroups("storage_formats").build(),

                /**
                 * We have docker images with KMS on CDH only. TODO (https://github.com/prestosql/presto/issues/1652) create images with HDP and KMS
                 *
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment singlenode-kerberos-kms-hdfs-impersonation \
                 *     -- -g storage_formats \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(SinglenodeKerberosKmsHdfsImpersonation.class).withGroups("storage_formats").build(),

                /**
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment singlenode-cassandra \
                 *     -- -g cassandra \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(SinglenodeCassandra.class).withGroups("cassandra").build(),

                /**
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment singlenode-kafka \
                 *     -- -g kafka \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(SinglenodeKafka.class).withGroups("kafka").build());
    }
}
