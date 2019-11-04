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
package io.prestosql.tests.hive;

import io.prestosql.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;

public class TestViewFsHiveTable
        extends ProductTest
{
    @Test(groups = STORAGE_FORMATS)
    public void testSelectViewFsTable()
    {
        onPresto().executeQuery("DROP TABLE IF EXISTS test_select_viewfs_base");
        onPresto().executeQuery("DROP TABLE IF EXISTS test_select_viewfs");

        onPresto().executeQuery("CREATE TABLE test_select_viewfs_base AS SELECT * FROM (VALUES (1, 'a')) AS t(col1, col2)");
        onPresto().executeQuery("" +
                "CREATE TABLE test_select_viewfs" +
                " (col1 INTEGER, col2 VARCHAR(1)) " +
                "WITH ( " +
                "   format = 'ORC', " +
                "   external_location = 'viewfs://hadoop-viewfs/default/test_select_viewfs_base'" +
                ")");
        assertThat(query("SELECT * FROM test_select_viewfs")).containsExactly(row(1, "a"));

        onPresto().executeQuery("DROP TABLE test_select_viewfs");
        onPresto().executeQuery("DROP TABLE test_select_viewfs_base");
    }
}
