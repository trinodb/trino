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

import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.HMS_ONLY;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWriteToHiveTransactionalTableInTrino
        extends ProductTest
{
    @Test(groups = {HMS_ONLY, PROFILE_SPECIFIC_TESTS})
    public void testInsertIntoUnpartitionedTable()
    {
        String tableName = "unpartitioned_transactional_insert";
        onTrino().executeQuery("CREATE TABLE %s (column1 INT) WITH (transactional = true)".formatted(tableName));
        onTrino().executeQuery("INSERT INTO %s VALUES (11)".formatted(tableName));
        assertThat(onTrino().executeQuery("SELECT * FROM %s".formatted(tableName))).containsOnly(row(11));
        onTrino().executeQuery("DROP TABLE %s".formatted(tableName));
    }

    @Test(groups = {HMS_ONLY, PROFILE_SPECIFIC_TESTS})
    public void testInsertIntoPartitionedTable()
    {
        String tableName = "partitioned_transactional_insert";
        onTrino().executeQuery("CREATE TABLE %s (column1 INT, column2 INT) WITH (transactional = true, partitioned_by = ARRAY['column2'])".formatted(tableName));
        onTrino().executeQuery("INSERT INTO %s VALUES (11, 12), (111, 121)".formatted(tableName));
        assertThat(onTrino().executeQuery("SELECT * FROM %s".formatted(tableName))).containsOnly(row(11, 12), row(111, 121));
        onTrino().executeQuery("DROP TABLE %s".formatted(tableName));
    }

    @Test(groups = {HMS_ONLY, PROFILE_SPECIFIC_TESTS})
    public void testInsertIntoNonPartitionedTable()
    {
        String tableName = "non_partitioned_transactional_insert";
        onTrino().executeQuery("CREATE TABLE %s (column1 INT, column2 INT) WITH (transactional = true)".formatted(tableName));
        onTrino().executeQuery("INSERT INTO %s VALUES (11, 12), (111, 121)".formatted(tableName));
        assertThat(onTrino().executeQuery("SELECT * FROM %s".formatted(tableName))).containsOnly(row(11, 12), row(111, 121));
        onTrino().executeQuery("DROP TABLE %s".formatted(tableName));
    }

    @Test(groups = {HMS_ONLY, PROFILE_SPECIFIC_TESTS})
    public void testUpdateOnUnpartitionedTable()
    {
        String tableName = "unpartitioned_transactional_update";
        onTrino().executeQuery("CREATE TABLE %s (column1 INT) WITH (transactional = true)".formatted(tableName));
        onTrino().executeQuery("INSERT INTO %s VALUES (11)".formatted(tableName));
        assertThat(onTrino().executeQuery("SELECT * FROM %s".formatted(tableName))).containsOnly(row(11));
        onTrino().executeQuery("UPDATE %s SET column1 = 999".formatted(tableName));
        assertThat(onTrino().executeQuery("SELECT * FROM %s".formatted(tableName))).containsOnly(row(999));
        onTrino().executeQuery("DROP TABLE %s".formatted(tableName));
    }

    @Test(groups = {HMS_ONLY, PROFILE_SPECIFIC_TESTS})
    public void testUpdateOnPartitionedTable()
    {
        String tableName = "partitioned_transactional_update";
        onTrino().executeQuery("CREATE TABLE %s (column1 INT, column2 INT) WITH (transactional = true, partitioned_by = ARRAY['column2'])".formatted(tableName));
        onTrino().executeQuery("INSERT INTO %s VALUES (11, 12), (111, 121)".formatted(tableName));
        assertThat(onTrino().executeQuery("SELECT * FROM %s".formatted(tableName))).containsOnly(row(11, 12), row(111, 121));
        onTrino().executeQuery("UPDATE %s SET column1 = 999".formatted(tableName));
        assertThat(onTrino().executeQuery("SELECT * FROM %s".formatted(tableName))).containsOnly(row(999, 12), row(999, 121));
        onTrino().executeQuery("UPDATE %s SET column1 = 321 WHERE column2 = 121".formatted(tableName));
        assertThat(onTrino().executeQuery("SELECT * FROM %s".formatted(tableName))).containsOnly(row(999, 12), row(321, 121));
        onTrino().executeQuery("DROP TABLE %s".formatted(tableName));
    }

    @Test(groups = {HMS_ONLY, PROFILE_SPECIFIC_TESTS})
    public void testDeleteOnUnpartitionedTable()
    {
        String tableName = "unpartitioned_transactional_delete";
        onTrino().executeQuery("CREATE TABLE %s (column1 INT) WITH (transactional = true)".formatted(tableName));
        onTrino().executeQuery("INSERT INTO %s VALUES (11)".formatted(tableName));
        assertThat(onTrino().executeQuery("SELECT * FROM %s".formatted(tableName))).containsOnly(row(11));
        onTrino().executeQuery("DELETE FROM %s".formatted(tableName));
        assertThat(onTrino().executeQuery("SELECT * FROM %s".formatted(tableName))).hasNoRows();
        onTrino().executeQuery("DROP TABLE %s".formatted(tableName));
    }

    @Test(groups = {HMS_ONLY, PROFILE_SPECIFIC_TESTS})
    public void testDeleteOnPartitionedTable()
    {
        String tableName = "partitioned_transactional_delete";
        onTrino().executeQuery("CREATE TABLE %s (column1 INT, column2 INT) WITH (transactional = true, partitioned_by = ARRAY['column2'])".formatted(tableName));
        onTrino().executeQuery("INSERT INTO %s VALUES (11, 12), (111, 121)".formatted(tableName));
        assertThat(onTrino().executeQuery("SELECT * FROM %s".formatted(tableName))).containsOnly(row(11, 12), row(111, 121));
        onTrino().executeQuery("DELETE FROM %s WHERE column2 = 121".formatted(tableName));
        assertThat(onTrino().executeQuery("SELECT * FROM %s".formatted(tableName))).containsOnly(row(11, 12));
        onTrino().executeQuery("DROP TABLE %s".formatted(tableName));
    }
}
