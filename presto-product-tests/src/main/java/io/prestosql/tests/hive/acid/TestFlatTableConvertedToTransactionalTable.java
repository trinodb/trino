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
package io.prestosql.tests.hive.acid;

import io.prestosql.tempto.query.QueryResult;
import io.prestosql.tests.hive.HiveProductTest;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.HIVE_TRANSACTIONAL;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.utils.QueryExecutors.onHive;

public class TestFlatTableConvertedToTransactionalTable
        extends HiveProductTest
{
    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL})
    public void testReadingFullAcidConvertedTable()
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Presto Hive transactional tables are supported with Hive version 3 or above");
        }

        String tableNameInDatabase = "reading_full_acid_converted_table";

        String createTable = "CREATE TABLE IF NOT EXISTS " +
                tableNameInDatabase +
                " (col INT, fcol INT) " +
                "STORED AS ORC TBLPROPERTIES ('transactional'='false')";
        onHive().executeQuery(createTable);

        try {
            onHive().executeQuery(
                    "INSERT OVERWRITE TABLE " + tableNameInDatabase + " select 1, 11");

            String selectAllFromTable = "SELECT * FROM " + tableNameInDatabase;
            QueryResult onePartitionQueryResult = query(selectAllFromTable);
            assertThat(onePartitionQueryResult).containsOnly(row(1, 11));

            onHive().executeQuery(
                    "INSERT INTO TABLE " + tableNameInDatabase + " select 2, 22");
            onePartitionQueryResult = query(selectAllFromTable);
            assertThat(onePartitionQueryResult).hasRowsCount(2);

            onHive().executeQuery(
                    "ALTER TABLE  " + tableNameInDatabase + " SET TBLPROPERTIES ('transactional'='true')");

            // delete a row
            onHive().executeQuery(
                    "DELETE FROM " + tableNameInDatabase + " WHERE fcol=22");
            onePartitionQueryResult = query(selectAllFromTable);
            assertThat(onePartitionQueryResult).containsOnly(row(1, 11));
            // update the existing row
            onHive().executeQuery(
                    "UPDATE " + tableNameInDatabase + " set col = 3 where fcol=11");
            onePartitionQueryResult = query(selectAllFromTable);
            assertThat(onePartitionQueryResult).containsOnly(row(3, 11));
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableNameInDatabase);
        }
    }

    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL})
    public void testReadingInsertOnlyAcidConvertedTable()
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Presto Hive transactional tables are supported with Hive version 3 or above");
        }

        String tableNameInDatabase = "reading_insert_only_acid_converted_table";

        String createTable = "CREATE TABLE IF NOT EXISTS " +
                tableNameInDatabase +
                " (col INT, fcol INT) " +
                "STORED AS ORC TBLPROPERTIES ('transactional'='false')";
        onHive().executeQuery(createTable);

        try {
            onHive().executeQuery(
                    "INSERT OVERWRITE TABLE " + tableNameInDatabase + " select 1, 11");

            String selectAllFromTable = "SELECT * FROM " + tableNameInDatabase;
            QueryResult onePartitionQueryResult = query(selectAllFromTable);
            assertThat(onePartitionQueryResult).containsOnly(row(1, 11));

            onHive().executeQuery(
                    "INSERT INTO TABLE " + tableNameInDatabase + " select 2, 22");
            onePartitionQueryResult = query(selectAllFromTable);
            assertThat(onePartitionQueryResult).hasRowsCount(2);

            onHive().executeQuery(
                    "ALTER TABLE  " + tableNameInDatabase + " SET TBLPROPERTIES ('transactional'='true','transactional_properties'='insert_only')");

            // insert a row
            onHive().executeQuery(
                    "INSERT INTO TABLE " + tableNameInDatabase + " select 3, 33");
            onePartitionQueryResult = query(selectAllFromTable);
            assertThat(onePartitionQueryResult).hasRowsCount(3);
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableNameInDatabase);
        }
    }
}
