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
package io.prestosql.tests.iceberg;

import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tests.TestGroups.ICEBERG;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;

public class TestIcebergCreateTable
        extends ProductTest
{
    @Test(groups = {ICEBERG, STORAGE_FORMATS})
    public void testCreateTable()
    {
        QueryExecutor queryExecutor = onPresto();
        queryExecutor.executeQuery("CREATE SCHEMA iceberg.iceberg");
        queryExecutor.executeQuery("use iceberg.iceberg");
        queryExecutor.executeQuery("CREATE TABLE test_create_table(a bigint, b varchar)");
        queryExecutor.executeQuery("INSERT INTO test_create_table(a, b) VALUES " +
                "(NULL, NULL), " +
                "(-42, 'abc'), " +
                "(9223372036854775807, 'abcdefghijklmnopqrstuvwxyz')");
        assertThat(queryExecutor.executeQuery("SELECT * FROM test_create_table"))
                .containsOnly(
                        row(null, null),
                        row(-42, "abc"),
                        row(9223372036854775807L, "abcdefghijklmnopqrstuvwxyz"));
        queryExecutor.executeQuery("DROP TABLE test_create_table");
        queryExecutor.executeQuery("DROP SCHEMA iceberg.iceberg");
    }

    @Test(groups = {ICEBERG, STORAGE_FORMATS})
    public void testCreateTableAsSelect()
    {
        QueryExecutor queryExecutor = onPresto();
        queryExecutor.executeQuery("CREATE SCHEMA iceberg.iceberg");
        queryExecutor.executeQuery("use iceberg.iceberg");
        queryExecutor.executeQuery("" +
                "CREATE TABLE test_create_table_as_select AS " +
                "SELECT * FROM (VALUES " +
                "  (NULL, NULL), " +
                "  (-42, 'abc'), " +
                "  (9223372036854775807, 'abcdefghijklmnopqrstuvwxyz')" +
                ") t(a, b)");
        assertThat(queryExecutor.executeQuery("SELECT * FROM test_create_table_as_select"))
                .containsOnly(
                        row(null, null),
                        row(-42, "abc"),
                        row(9223372036854775807L, "abcdefghijklmnopqrstuvwxyz"));
        queryExecutor.executeQuery("DROP TABLE test_create_table_as_select");
        queryExecutor.executeQuery("DROP SCHEMA iceberg.iceberg");
    }
}
