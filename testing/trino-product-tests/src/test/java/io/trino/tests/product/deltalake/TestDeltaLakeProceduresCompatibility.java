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
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JUnit 5 port of Delta Lake procedures compatibility tests from TestDeltaLakeProceduresCompatibility.
 * <p>
 * This class ports only the tests marked with DELTA_LAKE_OSS group.
 * Tests marked only with DELTA_LAKE_DATABRICKS are not included.
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeProceduresCompatibility
{
    @Test
    void testUnregisterTable(DeltaLakeMinioEnvironment env)
    {
        String bucketName = env.getBucketName();
        String tableName = "test_dl_unregister_table" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeTrinoUpdate(format("CREATE TABLE delta.default.%s WITH (location = 's3://%s/%s') AS SELECT 123 AS col",
                tableName,
                bucketName,
                tableDirectory));
        try {
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(row(123));
            assertThat(env.executeSpark("SELECT * FROM default." + tableName)).containsOnly(row(123));

            env.executeTrinoUpdate("CALL delta.system.unregister_table('default', '" + tableName + "')");

            assertThatThrownBy(() -> env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .hasMessageContaining("does not exist");
            assertThatThrownBy(() -> env.executeSpark("SELECT * FROM default." + tableName))
                    .hasStackTraceContaining("cannot be found");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + tableName);
        }
    }
}
