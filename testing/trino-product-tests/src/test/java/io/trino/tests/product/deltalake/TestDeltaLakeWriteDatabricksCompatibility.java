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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JUnit 5 port of Delta Lake write compatibility tests from TestDeltaLakeWriteDatabricksCompatibility.
 * <p>
 * This class ports only the tests marked with DELTA_LAKE_OSS group.
 * Tests marked only with DELTA_LAKE_DATABRICKS are not included.
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeWriteDatabricksCompatibility
{
    @Test
    void testVacuumProtocolCheck(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_vacuum_protocol_check_" + randomNameSuffix();
        String directoryName = "databricks-compatibility-test-" + tableName;

        env.executeSparkUpdate("CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/" + directoryName + "'" +
                "TBLPROPERTIES('delta.feature.vacuumProtocolCheck'='supported')");
        try {
            env.executeSparkUpdate("INSERT INTO " + tableName + " VALUES (1, 10)");
            env.executeSparkUpdate("UPDATE " + tableName + " SET a = 2");

            env.executeTrinoInSession(session -> {
                session.executeUpdate("SET SESSION delta.vacuum_min_retention = '0s'");
                session.executeUpdate("CALL delta.system.vacuum('default', '" + tableName + "', '0s')");
            });

            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(row(2, 10));
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(2, 10));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE default." + tableName);
        }
    }

    @Test
    void testUnsupportedWriterVersion(DeltaLakeMinioEnvironment env)
    {
        // Update this test and TestDeltaLakeBasic.testUnsupportedWriterVersion if Delta Lake OSS supports a new writer version
        String tableName = "test_dl_unsupported_writer_version_" + randomNameSuffix();

        assertThatThrownBy(() -> env.executeSparkUpdate("CREATE TABLE default." + tableName + "(col int) USING DELTA TBLPROPERTIES ('delta.minWriterVersion'='8')"))
                .hasStackTraceContaining("delta.minWriterVersion needs to be one of 1, 2, 3, 4, 5, 6, 7");

        // Clean up in case the table was somehow created
        env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
    }
}
