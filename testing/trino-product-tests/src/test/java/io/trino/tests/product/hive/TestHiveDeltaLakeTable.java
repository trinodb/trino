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
package io.trino.tests.product.hive;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JUnit 5 port of TestHiveDeltaLakeTable.
 * <p>
 * Tests that Trino properly rejects queries against Delta Lake tables
 * accessed through the Hive connector with a clear error message.
 */
@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
@TestGroup.StorageFormats
class TestHiveDeltaLakeTable
{
    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testReadDeltaLakeTable(HiveStorageFormatsEnvironment env)
    {
        String tableName = "test_delta_lake_table";

        env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);

        try {
            env.executeHiveUpdate(
                    "CREATE TABLE " + tableName + " (ignored int) " +
                            "TBLPROPERTIES ('spark.sql.sources.provider'='DELTA')");

            assertThatThrownBy(() -> env.executeTrino("SELECT * FROM hive.default." + tableName))
                    .hasStackTraceContaining("Cannot query Delta Lake table 'default." + tableName + "'");
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
