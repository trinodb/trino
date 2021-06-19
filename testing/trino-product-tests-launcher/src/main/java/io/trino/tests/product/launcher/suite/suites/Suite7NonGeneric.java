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
import io.trino.tests.product.launcher.env.environment.SinglenodeKerberosHdfsImpersonationCrossRealm;
import io.trino.tests.product.launcher.env.environment.SinglenodeMysql;
import io.trino.tests.product.launcher.env.environment.SinglenodePostgresql;
import io.trino.tests.product.launcher.env.environment.SinglenodeSparkHive;
import io.trino.tests.product.launcher.env.environment.SinglenodeSparkIceberg;
import io.trino.tests.product.launcher.env.environment.SinglenodeSqlserver;
import io.trino.tests.product.launcher.env.environment.TwoKerberosHives;
import io.trino.tests.product.launcher.env.environment.TwoMixedHives;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class Suite7NonGeneric
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        verify(config.getHadoopBaseImage().equals(EnvironmentDefaults.HADOOP_BASE_IMAGE), "The suite should be run with default HADOOP_BASE_IMAGE. Leave HADOOP_BASE_IMAGE unset.");

        return ImmutableList.of(
                testOnEnvironment(SinglenodeMysql.class).withGroups("mysql").build(),
                testOnEnvironment(SinglenodePostgresql.class).withGroups("postgresql").build(),
                testOnEnvironment(SinglenodeSqlserver.class).withGroups("sqlserver").build(),
                testOnEnvironment(SinglenodeSparkHive.class).withGroups("hive_spark_bucketing").build(),
                testOnEnvironment(SinglenodeSparkIceberg.class).withGroups("iceberg").withExcludedGroups("storage_formats").build(),
                testOnEnvironment(SinglenodeKerberosHdfsImpersonationCrossRealm.class).withGroups("storage_formats", "cli", "hdfs_impersonation").build(),
                testOnEnvironment(TwoMixedHives.class).withGroups("two_hives").build(),
                testOnEnvironment(TwoKerberosHives.class).withGroups("two_hives").build());
    }
}
