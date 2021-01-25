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
package io.trino.tests.hive;

import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.TestGroups.HIVE_VIEWS;
import static io.trino.tests.utils.QueryExecutors.onHive;

public class TestHiveViews
        extends AbstractTestHiveViews
{
    @Test(groups = HIVE_VIEWS)
    public void testSimpleCoral()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS hive_zero_index_view");
        onHive().executeQuery("DROP TABLE IF EXISTS hive_table_dummy");

        onHive().executeQuery("CREATE TABLE hive_table_dummy(a int)");
        onHive().executeQuery("CREATE VIEW hive_zero_index_view AS SELECT array('presto','hive')[1] AS sql_dialect FROM hive_table_dummy");
        onHive().executeQuery("INSERT INTO TABLE hive_table_dummy VALUES (1)");

        assertViewQuery(
                "SELECT * FROM hive_zero_index_view",
                queryAssert -> queryAssert.containsOnly(row("hive")));
    }
}
