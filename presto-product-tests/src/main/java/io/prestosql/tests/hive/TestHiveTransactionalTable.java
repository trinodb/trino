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

import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.utils.QueryExecutors.onHive;

public class TestHiveTransactionalTable
        extends ProductTest
{
    @Test
    public void testSelectFromTransactionalTable()
    {
        onHive().executeQuery("" +
                "CREATE TABLE test_select_from_transactional_table(a bigint)" +
                "CLUSTERED BY(a) INTO 4 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true', 'bucketing_version'='1')");
        try {
            assertThat(() -> query("SELECT * FROM test_select_from_transactional_table"))
                    .failsWithMessage("Hive transactional tables are not supported: default.test_select_from_transactional_table");
        }
        finally {
            onHive().executeQuery("DROP TABLE test_select_from_transactional_table");
        }
    }

    @Test
    public void testInsertIntoTransactionalTable()
    {
        onHive().executeQuery("" +
                "CREATE TABLE test_insert_into_transactional_table(a bigint)" +
                "CLUSTERED BY(a) INTO 4 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true', 'bucketing_version'='1')");

        try {
            assertThat(() -> query("INSERT INTO test_insert_into_transactional_table (a) VALUES (42)"))
                    .failsWithMessage("Hive transactional tables are not supported: default.test_insert_into_transactional_table");
        }
        finally {
            onHive().executeQuery("DROP TABLE test_insert_into_transactional_table");
        }
    }
}
