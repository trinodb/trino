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
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;

/**
 * Tests for writing to Hive transactional tables in Trino.
 * <p>
 * Ported from TestWriteToHiveTransactionalTableInTrino (Tempto-based test).
 */
@ProductTest
@RequiresEnvironment(HiveTransactionalEnvironment.class)
@TestGroup.HiveTransactional
class TestWriteToHiveTransactionalTableInTrino
{
    @Test
    void testInsertIntoUnpartitionedTable(HiveTransactionalEnvironment env)
    {
        String tableName = "unpartitioned_transactional_insert";
        env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (column1 INT) WITH (transactional = true)", tableName));
        try {
            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES (11)", tableName));
            assertThat(env.executeTrino(format("SELECT * FROM hive.default.%s", tableName))).containsOnly(row(11));
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE hive.default.%s", tableName));
        }
    }

    @Test
    void testInsertIntoPartitionedTable(HiveTransactionalEnvironment env)
    {
        String tableName = "partitioned_transactional_insert";
        env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (column1 INT, column2 INT) WITH (transactional = true, partitioned_by = ARRAY['column2'])", tableName));
        try {
            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES (11, 12), (111, 121)", tableName));
            assertThat(env.executeTrino(format("SELECT * FROM hive.default.%s", tableName))).containsOnly(row(11, 12), row(111, 121));
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE hive.default.%s", tableName));
        }
    }

    @Test
    void testInsertIntoNonPartitionedTable(HiveTransactionalEnvironment env)
    {
        String tableName = "non_partitioned_transactional_insert";
        env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (column1 INT, column2 INT) WITH (transactional = true)", tableName));
        try {
            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES (11, 12), (111, 121)", tableName));
            assertThat(env.executeTrino(format("SELECT * FROM hive.default.%s", tableName))).containsOnly(row(11, 12), row(111, 121));
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE hive.default.%s", tableName));
        }
    }

    @Test
    void testUpdateOnUnpartitionedTable(HiveTransactionalEnvironment env)
    {
        String tableName = "unpartitioned_transactional_update";
        env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (column1 INT) WITH (transactional = true)", tableName));
        try {
            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES (11)", tableName));
            assertThat(env.executeTrino(format("SELECT * FROM hive.default.%s", tableName))).containsOnly(row(11));
            env.executeTrinoUpdate(format("UPDATE hive.default.%s SET column1 = 999", tableName));
            assertThat(env.executeTrino(format("SELECT * FROM hive.default.%s", tableName))).containsOnly(row(999));
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE hive.default.%s", tableName));
        }
    }

    @Test
    void testUpdateOnPartitionedTable(HiveTransactionalEnvironment env)
    {
        String tableName = "partitioned_transactional_update";
        env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (column1 INT, column2 INT) WITH (transactional = true, partitioned_by = ARRAY['column2'])", tableName));
        try {
            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES (11, 12), (111, 121)", tableName));
            assertThat(env.executeTrino(format("SELECT * FROM hive.default.%s", tableName))).containsOnly(row(11, 12), row(111, 121));
            env.executeTrinoUpdate(format("UPDATE hive.default.%s SET column1 = 999", tableName));
            assertThat(env.executeTrino(format("SELECT * FROM hive.default.%s", tableName))).containsOnly(row(999, 12), row(999, 121));
            env.executeTrinoUpdate(format("UPDATE hive.default.%s SET column1 = 321 WHERE column2 = 121", tableName));
            assertThat(env.executeTrino(format("SELECT * FROM hive.default.%s", tableName))).containsOnly(row(999, 12), row(321, 121));
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE hive.default.%s", tableName));
        }
    }

    @Test
    void testDeleteOnUnpartitionedTable(HiveTransactionalEnvironment env)
    {
        String tableName = "unpartitioned_transactional_delete";
        env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (column1 INT) WITH (transactional = true)", tableName));
        try {
            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES (11)", tableName));
            assertThat(env.executeTrino(format("SELECT * FROM hive.default.%s", tableName))).containsOnly(row(11));
            env.executeTrinoUpdate(format("DELETE FROM hive.default.%s", tableName));
            assertThat(env.executeTrino(format("SELECT * FROM hive.default.%s", tableName))).hasNoRows();
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE hive.default.%s", tableName));
        }
    }

    @Test
    void testDeleteOnPartitionedTable(HiveTransactionalEnvironment env)
    {
        String tableName = "partitioned_transactional_delete";
        env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (column1 INT, column2 INT) WITH (transactional = true, partitioned_by = ARRAY['column2'])", tableName));
        try {
            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES (11, 12), (111, 121)", tableName));
            assertThat(env.executeTrino(format("SELECT * FROM hive.default.%s", tableName))).containsOnly(row(11, 12), row(111, 121));
            env.executeTrinoUpdate(format("DELETE FROM hive.default.%s WHERE column2 = 121", tableName));
            assertThat(env.executeTrino(format("SELECT * FROM hive.default.%s", tableName))).containsOnly(row(11, 12));
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE hive.default.%s", tableName));
        }
    }
}
