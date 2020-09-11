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
import io.prestosql.tests.product.launcher.env.environment.MultinodeHiveCaching;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeHiveImpersonation;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeKerberosHiveImpersonation;
import io.prestosql.tests.product.launcher.suite.Suite;
import io.prestosql.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static io.prestosql.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class Suite5
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        return ImmutableList.of(
                testOnEnvironment(SinglenodeHiveImpersonation.class).withGroups("storage_formats", "hdfs_impersonation").build(),
                testOnEnvironment(SinglenodeKerberosHiveImpersonation.class).withGroups("storage_formats", "hdfs_impersonation", "authorization").build(),
                testOnEnvironment(MultinodeHiveCaching.class)
                        .withGroups("hive_caching", "storage_formats")
                        .withExcludedGroups("iceberg")
                        .build());
    }
}
