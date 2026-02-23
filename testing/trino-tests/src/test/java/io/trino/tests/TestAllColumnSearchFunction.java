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
package io.trino.tests;

import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAllColumnSearchFunction
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build()).build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        return queryRunner;
    }

    @Test
    public void testBasicSearch()
    {
        // Search for 'UNITED' should return 2 nations
        assertThat(query(
                """
                SELECT name
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => 'UNITED',
                    case_sensitive => true))
                ORDER BY name
                """))
                .matches("SELECT name FROM tpch.tiny.nation WHERE name LIKE '%UNITED%' OR comment LIKE '%UNITED%' ORDER BY name");
    }

    @Test
    public void testCaseSensitiveSearch()
    {
        // Default is case-insensitive - lowercase search should match uppercase data
        assertThat(query(
                """
                SELECT name
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => 'united'))
                ORDER BY name
                """))
                .matches("SELECT name FROM tpch.tiny.nation WHERE name LIKE '%UNITED%' OR comment LIKE '%UNITED%' ORDER BY name");

        // Explicit case_sensitive => true - lowercase search should NOT match uppercase data
        assertThat(query(
                """
                SELECT name
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => 'united',
                    case_sensitive => true))
                """))
                .returnsEmptyResult();

        // Explicit case_sensitive => true - uppercase search should match uppercase data
        assertThat(query(
                """
                SELECT name
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => 'UNITED',
                    case_sensitive => true))
                ORDER BY name
                """))
                .matches("SELECT name FROM tpch.tiny.nation WHERE name LIKE '%UNITED%' OR comment LIKE '%UNITED%' ORDER BY name");

        // Explicit case_sensitive => false - lowercase search should match uppercase data
        assertThat(query(
                """
                SELECT name
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => 'united',
                    case_sensitive => false))
                ORDER BY name
                """))
                .matches("SELECT name FROM tpch.tiny.nation WHERE name LIKE '%UNITED%' OR comment LIKE '%UNITED%' ORDER BY name");
    }

    @Test
    public void testRegexPattern()
    {
        // Test regex pattern matching with case_sensitive => true
        assertThat(query(
                """
                SELECT name
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => '^UNITED.*',
                    case_sensitive => true))
                ORDER BY name
                """))
                .matches("SELECT name FROM tpch.tiny.nation WHERE REGEXP_LIKE(name, '^UNITED.*') OR REGEXP_LIKE(comment, '^UNITED.*') ORDER BY name");

        // Pattern matching any name ending with 'IA'
        assertThat(query(
                """
                SELECT name
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => '.*IA$',
                    case_sensitive => true))
                ORDER BY name
                """))
                .matches("SELECT name FROM tpch.tiny.nation WHERE REGEXP_LIKE(name, '.*IA$') OR REGEXP_LIKE(comment, '.*IA$') ORDER BY name");
    }

    @Test
    public void testNoMatches()
    {
        // Search for non-existent term should return empty result
        assertThat(query(
                """
                SELECT *
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => 'NONEXISTENT'))
                """))
                .returnsEmptyResult();
    }

    @Test
    public void testSearchAcrossMultipleColumns()
    {
        // Search in comment column - should find matches
        assertThat(query(
                """
                SELECT name
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => 'carefully',
                    case_sensitive => true))
                """))
                .matches("SELECT name FROM tpch.tiny.nation WHERE comment LIKE '%carefully%'");
    }

    @Test
    public void testAllRowsMatch()
    {
        // Search for very common pattern
        assertThat(query(
                """
                SELECT COUNT(*)
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => '.',
                    case_sensitive => true))
                """))
                .matches("SELECT BIGINT '25'");
    }

    @Test
    public void testWithLimit()
    {
        // Test with LIMIT clause
        assertThat(query(
                """
                SELECT name
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => 'A',
                    case_sensitive => true))
                LIMIT 3
                """))
                .matches("SELECT name FROM tpch.tiny.nation WHERE name LIKE '%A%' OR comment LIKE '%A%' LIMIT 3");
    }

    @Test
    public void testWithJoin()
    {
        // Test function in JOIN
        assertThat(query(
                """
                SELECT n.name, r.name
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => 'UNITED',
                    case_sensitive => true)) n
                JOIN tpch.tiny.region r ON n.regionkey = r.regionkey
                ORDER BY n.name
                """))
                .matches(
                        """
                        SELECT n.name, r.name
                        FROM tpch.tiny.nation n
                        JOIN tpch.tiny.region r ON n.regionkey = r.regionkey
                        WHERE n.name LIKE '%UNITED%'
                        ORDER BY n.name
                        """);
    }

    @Test
    public void testWithWhereClause()
    {
        // Test combining with WHERE clause
        assertThat(query(
                """
                SELECT name
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => 'A',
                    case_sensitive => true))
                WHERE regionkey = 1
                """))
                .matches("SELECT name FROM tpch.tiny.nation WHERE (name LIKE '%A%' OR comment LIKE '%A%') AND regionkey = 1");
    }

    @Test
    public void testLargeTable()
    {
        // Test with larger table (customer has 1500 rows in tiny)
        assertThat(query(
                """
                SELECT COUNT(*)
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.customer),
                    search_term => 'Customer',
                    case_sensitive => true))
                """))
                .matches("SELECT COUNT(*) FROM tpch.tiny.customer WHERE name LIKE '%Customer%' OR mktsegment LIKE '%Customer%' OR comment LIKE '%Customer%'");
    }

    @Test
    public void testEmptySearchTerm()
    {
        // Empty search term should fail
        assertThat(query(
                """
                SELECT *
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => ''))
                """))
                .failure().hasMessage("Search term cannot be empty");
    }

    @Test
    public void testNullSearchTerm()
    {
        // Null search term should fail
        assertThat(query(
                """
                SELECT *
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => CAST(null AS VARCHAR)))
                """))
                .failure().hasMessage("Search term cannot be null");
    }

    @Test
    public void testInvalidRegexPattern()
    {
        // Invalid regex pattern should fail
        assertThat(query(
                """
                SELECT *
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => '[invalid'))
                """))
                .failure().hasMessageContaining("Invalid regex pattern");
    }

    @Test
    public void testTableWithNoStringColumns()
    {
        // Table with only numeric columns should fail
        assertThat(query(
                """
                SELECT *
                FROM TABLE(allcolumnsearch(
                    input => TABLE(SELECT 1, 2, 3),
                    search_term => 'test'))
                """))
                .failure().hasMessage("No searchable string columns found in input table");
    }

    @Test
    public void testFunctionResolution()
    {
        // Test fully qualified function name
        assertThat(query(
                """
                SELECT name
                FROM TABLE(system.builtin.allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => 'UNITED',
                    case_sensitive => true))
                ORDER BY name
                """))
                .matches(
                        """
                        SELECT name
                        FROM TABLE(allcolumnsearch(
                            input => TABLE(tpch.tiny.nation),
                            search_term => 'UNITED',
                            case_sensitive => true))
                        ORDER BY name
                        """);
    }

    @Test
    public void testSpecialCharacters()
    {
        // Test with special regex characters in search term
        assertThat(query(
                """
                SELECT *
                FROM TABLE(allcolumnsearch(
                    input => TABLE(SELECT 'test.value' AS col),
                    search_term => '\\.',
                    case_sensitive => true))
                """))
                .matches("SELECT * FROM (SELECT 'test.value' AS col) WHERE REGEXP_LIKE(col, '\\.')");
    }

    @Test
    public void testSubquery()
    {
        // Test with subquery as input
        assertThat(query(
                """
                SELECT name
                FROM TABLE(allcolumnsearch(
                    input => TABLE(SELECT * FROM tpch.tiny.nation WHERE regionkey = 1),
                    search_term => 'A',
                    case_sensitive => true))
                """))
                .matches("SELECT name FROM tpch.tiny.nation WHERE regionkey = 1 AND (name LIKE '%A%' OR comment LIKE '%A%')");
    }

    @Test
    public void testNullValuesInColumns()
    {
        // Test handling of NULL values in searchable columns
        assertThat(query(
                """
                SELECT *
                FROM TABLE(allcolumnsearch(
                    input => TABLE(SELECT CAST(NULL AS VARCHAR) AS col1, 'value' AS col2),
                    search_term => 'value',
                    case_sensitive => true))
                """))
                .matches("SELECT * FROM (SELECT CAST(NULL AS VARCHAR) AS col1, 'value' AS col2) WHERE col1 LIKE '%value%' OR col2 LIKE '%value%'");

        // NULL values should be skipped, not cause errors
        assertThat(query(
                """
                SELECT *
                FROM TABLE(allcolumnsearch(
                    input => TABLE(SELECT CAST(NULL AS VARCHAR) AS col1, CAST(NULL AS VARCHAR) AS col2),
                    search_term => 'test',
                    case_sensitive => true))
                """))
                .returnsEmptyResult();
    }

    @Test
    public void testPartialMatch()
    {
        // Test partial string matching
        assertThat(query(
                """
                SELECT name
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => 'GER',
                    case_sensitive => true))
                ORDER BY name
                """))
                .matches("SELECT name FROM tpch.tiny.nation WHERE name LIKE '%GER%' OR comment LIKE '%GER%' ORDER BY name");
    }

    // ==================== Additional Failure Test Cases ====================

    @Test
    public void testMissingInputParameter()
    {
        // Missing required 'input' parameter should fail
        assertThat(query(
                """
                SELECT *
                FROM TABLE(allcolumnsearch(
                    search_term => 'test'))
                """))
                .failure().hasMessageContaining("Missing argument");
    }

    @Test
    public void testMissingSearchTermParameter()
    {
        // Missing required 'search_term' parameter should fail
        assertThat(query(
                """
                SELECT *
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation)))
                """))
                .failure().hasMessageContaining("Missing argument");
    }

    @Test
    public void testInvalidSearchTermType()
    {
        // Passing integer instead of string should fail
        assertThat(query(
                """
                SELECT *
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => 123))
                """))
                .failure().hasMessageContaining("Cannot cast type integer to varchar");
    }

    @Test
    public void testInvalidCaseSensitiveType()
    {
        // Passing string instead of boolean for case_sensitive should fail
        assertThat(query(
                """
                SELECT *
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => 'test',
                    case_sensitive => 'yes'))
                """))
                .failure().hasMessageContaining("Cannot cast type varchar(3) to boolean");
    }


    @Test
    public void testUnbalancedBrackets()
    {
        // Unbalanced brackets in regex should fail
        assertThat(query(
                """
                SELECT *
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => '[a-z'))
                """))
                .failure().hasMessageContaining("Invalid regex pattern");
    }

    @Test
    public void testWhitespaceOnlySearchTerm()
    {
        // Search term with only whitespace should be treated as a valid pattern
        // This tests that whitespace-only terms are allowed (they're valid regex)
        assertThat(query(
                """
                SELECT COUNT(*)
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => '   ',
                    case_sensitive => true))
                """))
                .matches("SELECT COUNT(*) FROM tpch.tiny.nation WHERE name LIKE '%   %' OR comment LIKE '%   %'");
    }

    @Test
    public void testNegativeCaseForCaseInsensitivity()
    {
        // Verify case_sensitive => false (default) actually works case-insensitively
        // Search for 'germany' (lowercase) should match 'GERMANY' (uppercase) in data
        assertThat(query(
                """
                SELECT name
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => 'germany',
                    case_sensitive => false))
                """))
                .matches("SELECT name FROM tpch.tiny.nation WHERE LOWER(name) LIKE '%germany%' OR LOWER(comment) LIKE '%germany%'");
    }

    @Test
    public void testRegexWithBackslashEscape()
    {
        // Test proper handling of backslash in regex patterns
        // The search_term '\\' (two backslashes in SQL string literal = one backslash regex pattern)
        // should match the backslash character in the data
        assertThat(query(
                """
                SELECT col
                FROM TABLE(allcolumnsearch(
                    input => TABLE(SELECT 'test\\value' AS col),
                    search_term => '\\\\',
                    case_sensitive => true))
                """))
                .matches("VALUES ('test\\value')");
    }

    @Test
    public void testEmptyTableInput()
    {
        // Empty table should return empty result, not fail
        assertThat(query(
                """
                SELECT *
                FROM TABLE(allcolumnsearch(
                    input => TABLE(SELECT name FROM tpch.tiny.nation WHERE 1=0),
                    search_term => 'test'))
                """))
                .returnsEmptyResult();
    }

    @Test
    public void testMultipleRegexFlags()
    {
        // Test case-insensitive mode with complex regex
        assertThat(query(
                """
                SELECT name
                FROM TABLE(allcolumnsearch(
                    input => TABLE(tpch.tiny.nation),
                    search_term => '^u.*d$',
                    case_sensitive => false))
                ORDER BY name
                """))
                .matches("SELECT name FROM tpch.tiny.nation WHERE REGEXP_LIKE(LOWER(name), '^u.*d$') OR REGEXP_LIKE(LOWER(comment), '^u.*d$') ORDER BY name");
    }
}
