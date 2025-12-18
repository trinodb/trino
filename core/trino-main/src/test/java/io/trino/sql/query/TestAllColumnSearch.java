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
package io.trino.sql.query;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestAllColumnSearch
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testBasicSearch()
    {
        // Create test table with VARCHAR columns - case-sensitive search
        assertThat(assertions.query("""
                SELECT * FROM (
                    VALUES
                        (1, CAST('error occurred' AS VARCHAR), CAST('System message' AS VARCHAR), 100),
                        (2, CAST('Success' AS VARCHAR), CAST('Operation completed' AS VARCHAR), 200),
                        (3, CAST('Warning' AS VARCHAR), CAST('error detected in logs' AS VARCHAR), 300)
                ) t(id, name, description, value)
                WHERE allcolumnsearch('error')
                """))
                .matches("""
                        VALUES
                            (1, VARCHAR 'error occurred', VARCHAR 'System message', 100),
                            (3, VARCHAR 'Warning', VARCHAR 'error detected in logs', 300)
                        """);
    }

    @Test
    public void testCaseSensitive()
    {
        // Verify case-sensitive matching
        assertThat(assertions.query("""
                SELECT * FROM (
                    VALUES
                        (1, CAST('ERROR' AS VARCHAR)),
                        (2, CAST('error' AS VARCHAR)),
                        (3, CAST('Error' AS VARCHAR)),
                        (4, CAST('success' AS VARCHAR))
                ) t(id, message)
                WHERE allcolumnsearch('error')
                """))
                .matches("""
                        VALUES
                            (2, VARCHAR 'error')
                        """);
    }

    @Test
    public void testSubstringMatch()
    {
        // Verify substring matching
        assertThat(assertions.query("""
                SELECT * FROM (
                    VALUES
                        (1, CAST('user123' AS VARCHAR)),
                        (2, CAST('test456' AS VARCHAR)),
                        (3, CAST('user789' AS VARCHAR))
                ) t(id, username)
                WHERE allcolumnsearch('user')
                """))
                .matches("""
                        VALUES
                            (1, VARCHAR 'user123'),
                            (3, VARCHAR 'user789')
                        """);
    }

    @Test
    public void testNoMatches()
    {
        // Verify no matches returns empty result
        assertThat(assertions.query("""
                SELECT * FROM (
                    VALUES
                        (1, CAST('apple' AS VARCHAR)),
                        (2, CAST('banana' AS VARCHAR)),
                        (3, CAST('cherry' AS VARCHAR))
                ) t(id, fruit)
                WHERE allcolumnsearch('orange')
                """))
                .returnsEmptyResult();
    }

    @Test
    public void testWithOtherPredicates()
    {
        // Verify combination with other WHERE conditions - case-sensitive
        assertThat(assertions.query("""
                SELECT * FROM (
                    VALUES
                        (1, CAST('error A' AS VARCHAR), 100),
                        (2, CAST('error B' AS VARCHAR), 200),
                        (3, CAST('Success' AS VARCHAR), 300)
                ) t(id, message, value)
                WHERE allcolumnsearch('error') AND value > 150
                """))
                .matches("VALUES (2, VARCHAR 'error B', 200)");
    }

    @Test
    public void testWithNullValues()
    {
        // Verify NULL handling - case-sensitive
        assertThat(assertions.query("""
                SELECT * FROM (
                    VALUES
                        (1, CAST('error' AS VARCHAR), CAST(NULL AS VARCHAR)),
                        (2, CAST(NULL AS VARCHAR), CAST('error message' AS VARCHAR)),
                        (3, CAST('Success' AS VARCHAR), CAST('OK' AS VARCHAR))
                ) t(id, col1, col2)
                WHERE allcolumnsearch('error')
                """))
                .matches("""
                        VALUES
                            (1, VARCHAR 'error', CAST(NULL AS VARCHAR)),
                            (2, CAST(NULL AS VARCHAR), VARCHAR 'error message')
                        """);
    }

    @Test
    public void testMultipleColumns()
    {
        // Test with table having many VARCHAR columns
        assertThat(assertions.query("""
                SELECT id FROM (
                    VALUES
                        (1, CAST('A' AS VARCHAR), CAST('B' AS VARCHAR), CAST('C' AS VARCHAR), CAST('found' AS VARCHAR)),
                        (2, CAST('D' AS VARCHAR), CAST('E' AS VARCHAR), CAST('F' AS VARCHAR), CAST('G' AS VARCHAR)),
                        (3, CAST('H' AS VARCHAR), CAST('found' AS VARCHAR), CAST('J' AS VARCHAR), CAST('K' AS VARCHAR))
                ) t(id, col1, col2, col3, col4)
                WHERE allcolumnsearch('found')
                """))
                .matches("VALUES 1, 3");
    }

    @Test
    public void testMixedColumnTypes()
    {
        // Verify only VARCHAR/CHAR columns are searched, not integers
        assertThat(assertions.query("""
                SELECT * FROM (
                    VALUES
                        (123, CAST('test' AS VARCHAR)),
                        (456, CAST('not found' AS VARCHAR)),
                        (123, CAST('contains 123' AS VARCHAR))
                ) t(number, text)
                WHERE allcolumnsearch('123')
                """))
                .matches("VALUES (123, VARCHAR 'contains 123')");
    }

    @Test
    public void testEmptyString()
    {
        // Test searching for empty string (should fail)
        assertThat(assertions.query("""
                SELECT * FROM (VALUES (1, 'test')) t(id, col)
                WHERE allcolumnsearch('')
                """))
                .failure().hasMessageContaining("search term cannot be empty");
    }

    @Test
    public void testWithJoin()
    {
        // Test allcolumnsearch with joins - searches columns from all joined tables
        assertThat(assertions.query("""
                SELECT t1.id FROM
                    (VALUES (1, CAST('error' AS VARCHAR)), (2, CAST('success' AS VARCHAR))) t1(id, msg),
                    (VALUES (1, CAST('A' AS VARCHAR)), (2, CAST('B' AS VARCHAR))) t2(id, val)
                WHERE t1.id = t2.id AND allcolumnsearch('error')
                """))
                .matches("VALUES 1");
    }

    @Test
    public void testInSubquery()
    {
        // Test allcolumnsearch in subquery
        assertThat(assertions.query("""
                SELECT * FROM (
                    SELECT * FROM (
                        VALUES (1, CAST('error' AS VARCHAR)), (2, CAST('success' AS VARCHAR))
                    ) t(id, message)
                    WHERE allcolumnsearch('error')
                ) subq
                """))
                .matches("VALUES (1, VARCHAR 'error')");
    }

    @Test
    public void testSpecialCharacters()
    {
        // Test with special characters
        assertThat(assertions.query("""
                SELECT * FROM (
                    VALUES
                        (1, CAST('test@example.com' AS VARCHAR)),
                        (2, CAST('user@domain.com' AS VARCHAR)),
                        (3, CAST('noemail' AS VARCHAR))
                ) t(id, email)
                WHERE allcolumnsearch('@')
                """))
                .matches("""
                        VALUES
                            (1, VARCHAR 'test@example.com'),
                            (2, VARCHAR 'user@domain.com')
                        """);
    }

    @Test
    public void testWithOrderBy()
    {
        // Test with ORDER BY clause - case-sensitive
        assertThat(assertions.query("""
                SELECT id FROM (
                    VALUES
                        (3, CAST('error C' AS VARCHAR)),
                        (1, CAST('error A' AS VARCHAR)),
                        (2, CAST('error B' AS VARCHAR)),
                        (4, CAST('ERROR D' AS VARCHAR))
                ) t(id, message)
                WHERE allcolumnsearch('error')
                ORDER BY id
                """))
                .matches("VALUES 1, 2, 3");
    }

    @Test
    public void testWithLimit()
    {
        // Test with LIMIT clause
        assertThat(assertions.query("""
                SELECT * FROM (
                    VALUES
                        (1, CAST('test' AS VARCHAR)),
                        (2, CAST('test' AS VARCHAR)),
                        (3, CAST('test' AS VARCHAR))
                ) t(id, message)
                WHERE allcolumnsearch('test')
                LIMIT 2
                """))
                .matches("""
                        VALUES
                            (1, VARCHAR 'test'),
                            (2, VARCHAR 'test')
                        """);
    }
}
