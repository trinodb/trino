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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_MATCH;

@ProductTest
@RequiresEnvironment(DeltaLakeDatabricksEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeDatabricks
@TestGroup.ProfileSpecificTests
class TestDeltaLakeJmxDatabricks
{
    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testJmxTablesExposedByDeltaLakeConnectorBackedByGlueMetastore(DeltaLakeDatabricksEnvironment env)
    {
        assertThat(env.executeTrinoSql("SHOW TABLES IN jmx.current LIKE '%name=delta%'"))
                .containsOnly(
                        row("io.airlift.bootstrap:name=delta,type=lifecyclemanager"),
                        row("io.trino.filesystem.s3:name=delta,type=s3filesystemstats"),
                        row("io.trino.metastore.cache:name=delta,type=cachinghivemetastore"),
                        row("io.trino.plugin.hive.metastore.glue:name=delta,type=gluehivemetastore"),
                        row("io.trino.plugin.hive.metastore.glue:name=delta,type=gluemetastorestats"),
                        row("io.trino.plugin.base.metrics:catalog=delta,name=delta,type=fileformatdatasourcestats"),
                        row("trino.plugin.deltalake.metastore:catalog=delta,name=delta,type=deltalaketablemetadatascheduler"),
                        row("trino.plugin.deltalake.transactionlog:catalog=delta,name=delta,type=transactionlogaccess"));
    }
}
