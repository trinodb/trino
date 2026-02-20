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
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;

/**
 * Tests for Delta Lake time travel compatibility with Spark/Delta OSS.
 * <p>
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeTimeTravelCompatibility
{
    @Test
    void testReadFromTableRestoredToPreviousVersion(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_time_travel_restore_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeTrinoUpdate("CREATE TABLE delta.default." + tableName + " (a_integer integer) WITH (location = 's3://" + env.getBucketName() + "/" + tableDirectory + "')");

        try {
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES 1");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES 2");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES 3");
            env.executeTrinoUpdate("UPDATE delta.default." + tableName + " SET a_integer = a_integer + 10 WHERE a_integer > 1");
            env.executeTrinoUpdate("DELETE FROM delta.default." + tableName + " WHERE a_integer < 10");
            env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " ADD COLUMN a_varchar varchar");
            env.executeTrinoUpdate("UPDATE delta.default." + tableName + " SET a_varchar = 'foo'");

            List<Row> expectedRows = List.of(
                    row(12, "foo"),
                    row(13, "foo"));
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            env.executeSparkUpdate("RESTORE TABLE default." + tableName + " TO VERSION AS OF 1");
            expectedRows = List.of(row(1));

            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testSelectForVersionAsOf(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_select_version" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (id INT, part STRING)" +
                "USING delta " +
                "PARTITIONED BY (part)" +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 'spark')");
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " ADD COLUMN new_column INT");

            // Both Spark and Trino Delta Lake connector use the old table definition for the versioned query
            List<Row> expectedRows = List.of(row(1, "spark"));
            assertThat(env.executeSpark("SELECT * FROM default." + tableName + " VERSION AS OF 1"))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName + " FOR VERSION AS OF 1"))
                    .containsOnly(expectedRows);

            // Do time travel after table replacement
            env.executeSparkUpdate("CREATE OR REPLACE TABLE " + tableName + " USING DELTA AS SELECT id + 1 AS id, part, new_column FROM " + tableName);
            assertThat(env.executeSpark("SELECT * FROM default." + tableName + " VERSION AS OF 1"))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName + " FOR VERSION AS OF 1"))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testSelectForTemporalAsOf(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_select_temporal_" + randomNameSuffix();
        DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        DateTimeFormatter timestampWithTimeZoneFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS VV");

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(id INT, part STRING) " +
                "USING delta " +
                "PARTITIONED BY (part)" +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 'spark')");
            ZonedDateTime timeAfterInsert = ZonedDateTime.now(ZoneId.of("UTC"));

            env.executeSparkUpdate("ALTER TABLE default." + tableName + " ADD COLUMN new_column INT");

            // Both Spark and Trino Delta Lake connector use the old table definition for the versioned query
            List<Row> expectedRows = List.of(row(1, "spark"));
            assertThat(env.executeSpark("SELECT * FROM default." + tableName + " TIMESTAMP AS OF '" + timeAfterInsert.format(timestampFormatter) + "'"))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName + " FOR TIMESTAMP AS OF TIMESTAMP '" + timeAfterInsert.format(timestampWithTimeZoneFormatter) + "'"))
                    .containsOnly(expectedRows);

            // Do time travel after table replacement
            env.executeSparkUpdate("CREATE OR REPLACE TABLE " + tableName + " USING DELTA AS SELECT id + 1 AS id, part, new_column FROM " + tableName);
            assertThat(env.executeSpark("SELECT * FROM default." + tableName + " TIMESTAMP AS OF '" + timeAfterInsert.format(timestampFormatter) + "'"))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName + " FOR TIMESTAMP AS OF TIMESTAMP '" + timeAfterInsert.format(timestampWithTimeZoneFormatter) + "'"))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }
}
