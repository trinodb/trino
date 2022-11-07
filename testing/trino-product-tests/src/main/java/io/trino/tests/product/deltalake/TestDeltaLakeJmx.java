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
package io.trino.tests.product.deltalake;

import io.trino.tempto.ProductTest;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestDeltaLakeJmx
        extends ProductTest
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testJmxTablesExposedByDeltaLakeConnectorBackedByGlueMetastore()
    {
        assertThat(onTrino().executeQuery("SHOW TABLES IN jmx.current LIKE '%name=delta%'")).containsOnly(
                row("io.trino.plugin.hive.metastore.cache:name=delta,type=cachinghivemetastore"),
                row("io.trino.plugin.hive.metastore.glue:name=delta,type=gluehivemetastore"),
                row("io.trino.plugin.hive.s3:name=delta,type=trinos3filesystem"),
                row("io.trino.plugin.hive:catalog=delta,name=delta,type=fileformatdatasourcestats"),
                row("trino.plugin.deltalake.transactionlog:catalog=delta,name=delta,type=transactionlogaccess"));
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testJmxTablesExposedByDeltaLakeConnectorBackedByThriftMetastore()
    {
        assertThat(onTrino().executeQuery("SHOW TABLES IN jmx.current LIKE '%name=delta%'")).containsOnly(
                row("io.trino.plugin.hive.metastore.cache:name=delta,type=cachinghivemetastore"),
                row("io.trino.plugin.hive.metastore.thrift:name=delta,type=thrifthivemetastore"),
                row("io.trino.plugin.hive.s3:name=delta,type=trinos3filesystem"),
                row("io.trino.plugin.hive:catalog=delta,name=delta,type=fileformatdatasourcestats"),
                row("trino.plugin.deltalake.transactionlog:catalog=delta,name=delta,type=transactionlogaccess"));
    }
}
