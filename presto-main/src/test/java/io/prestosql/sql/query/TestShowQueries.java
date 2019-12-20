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
package io.prestosql.sql.query;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestShowQueries
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testShowCatalogsLikeWithEscape()
    {
        assertions.assertFails("SHOW CATALOGS LIKE 't$_%' ESCAPE ''", "Escape string must be a single character");
        assertions.assertFails("SHOW CATALOGS LIKE 't$_%' ESCAPE '$$'", "Escape string must be a single character");
        assertions.assertQuery("SHOW CATALOGS LIKE '%$_%' ESCAPE '$'", "VALUES('testing_catalog')");
        assertions.assertQuery("SHOW CATALOGS LIKE '$_%' ESCAPE '$'", "SELECT 'testing_catalog' WHERE FALSE");
    }

    @Test
    public void testShowFunctionLike()
    {
        assertions.assertQuery("SHOW FUNCTIONS LIKE 'split%'",
                "VALUES " +
                        "(cast('split' AS VARCHAR(24)), cast('array(varchar(x))' AS VARCHAR(28)), cast('varchar(x), varchar(y)' AS VARCHAR(62)), cast('scalar' AS VARCHAR(9)), true, cast('' AS VARCHAR(131)))," +
                        "('split', 'array(varchar(x))', 'varchar(x), varchar(y), bigint', 'scalar', true, '')," +
                        "('split_part', 'varchar(x)', 'varchar(x), varchar(y), bigint', 'scalar', true, 'splits a string by a delimiter and returns the specified field (counting from one)')," +
                        "('split_to_map', 'map(varchar,varchar)', 'varchar, varchar, varchar', 'scalar', true, 'creates a map using entryDelimiter and keyValueDelimiter')," +
                        "('split_to_multimap', 'map(varchar,array(varchar))', 'varchar, varchar, varchar', 'scalar', true, 'creates a multimap by splitting a string into key/value pairs')");
    }

    @Test
    public void testShowFunctionsLikeWithEscape()
    {
        assertions.assertQuery("SHOW FUNCTIONS LIKE 'split$_to$_%' ESCAPE '$'",
                "VALUES " +
                        "(cast('split_to_map' AS VARCHAR(24)), cast('map(varchar,varchar)' AS VARCHAR(28)), cast('varchar, varchar, varchar' AS VARCHAR(62)), cast('scalar' AS VARCHAR(9)), true, cast('creates a map using entryDelimiter and keyValueDelimiter' AS VARCHAR(131)))," +
                        "('split_to_multimap', 'map(varchar,array(varchar))', 'varchar, varchar, varchar', 'scalar', true, 'creates a multimap by splitting a string into key/value pairs')");
    }

    @Test
    public void testShowSessionLike()
    {
        assertions.assertQuery(
                "SHOW SESSION LIKE '%page_row_c%'",
                "VALUES ('filter_and_project_min_output_page_row_count', cast('256' as VARCHAR(21)), cast('256' as VARCHAR(21)), 'integer', cast('Experimental: Minimum output page row count for filter and project operators' as VARCHAR(118)))");
    }

    @Test
    public void testShowSessionLikeWithEscape()
    {
        assertions.assertFails("SHOW SESSION LIKE 't$_%' ESCAPE ''", "Escape string must be a single character");
        assertions.assertFails("SHOW SESSION LIKE 't$_%' ESCAPE '$$'", "Escape string must be a single character");
        assertions.assertQuery(
                "SHOW SESSION LIKE '%page$_row$_c%' ESCAPE '$'",
                "VALUES ('filter_and_project_min_output_page_row_count', cast('256' as VARCHAR(21)), cast('256' as VARCHAR(21)), 'integer', cast('Experimental: Minimum output page row count for filter and project operators' as VARCHAR(118)))");
    }
}
