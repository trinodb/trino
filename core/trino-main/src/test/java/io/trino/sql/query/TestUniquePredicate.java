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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestUniquePredicate
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testDistinctRowsAreUnique()
    {
        assertThat(assertions.query("SELECT UNIQUE (VALUES 1, 2, 3)"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT UNIQUE (VALUES (1, 'a'), (2, 'b'), (3, 'c'))"))
                .matches("VALUES true");
    }

    @Test
    public void testDuplicateRowsAreNotUnique()
    {
        assertThat(assertions.query("SELECT UNIQUE (VALUES 1, 1, 2)"))
                .matches("VALUES false");
        assertThat(assertions.query("SELECT UNIQUE (VALUES (1, 'a'), (1, 'a'))"))
                .matches("VALUES false");
    }

    @Test
    public void testEmptySubqueryIsUnique()
    {
        // Empty table satisfies UNIQUE trivially (SQL:2003 §8.9 case a)
        assertThat(assertions.query("SELECT UNIQUE (SELECT 1 WHERE false)"))
                .matches("VALUES true");
    }

    @Test
    public void testNullRowsDoNotCountAsDuplicates()
    {
        // Rows containing a NULL are never duplicates of anything — including each other.
        // Two (NULL, 'a') rows are NOT duplicates.
        assertThat(assertions.query("SELECT UNIQUE (VALUES (CAST(NULL AS INTEGER), 'a'), (CAST(NULL AS INTEGER), 'a'))"))
                .matches("VALUES true");
        // Mixing duplicates of non-null rows with NULL rows: the non-null duplicate kills uniqueness.
        assertThat(assertions.query("SELECT UNIQUE (VALUES (CAST(NULL AS INTEGER), 'a'), (1, 'b'), (1, 'b'))"))
                .matches("VALUES false");
    }

    @Test
    public void testAllRowsContainNullsIsUnique()
    {
        // Every row has a NULL → no duplicate pairs possible → TRUE.
        assertThat(assertions.query("SELECT UNIQUE (VALUES (CAST(NULL AS INTEGER), 1), (2, CAST(NULL AS INTEGER)))"))
                .matches("VALUES true");
    }

    @Test
    public void testNotUnique()
    {
        assertThat(assertions.query("SELECT NOT UNIQUE (VALUES 1, 1)"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT NOT UNIQUE (VALUES 1, 2)"))
                .matches("VALUES false");
    }

    @Test
    public void testUniqueInWhereClause()
    {
        assertThat(assertions.query(
                "SELECT a FROM (VALUES 1, 2) t(a) WHERE UNIQUE (VALUES 1, 2, 3)"))
                .matches("VALUES 1, 2");
        assertThat(assertions.query(
                "SELECT a FROM (VALUES 1, 2) t(a) WHERE UNIQUE (VALUES 1, 1)"))
                .returnsEmptyResult();
    }

    @Test
    public void testUniqueInExtendedCaseWhen()
    {
        // UNIQUE has no LHS, so in extended SIMPLE CASE WHEN it does not appear as a predicate
        // fragment (the SQL:2023 F262 fragment set only includes predicates whose operand can
        // be made implicit). It still works inside a SEARCHED CASE WHEN, which accepts any
        // boolean expression — no extension needed.
        assertThat(assertions.query(
                "SELECT CASE " +
                        "  WHEN UNIQUE (VALUES 1, 2, 3) THEN 'distinct' " +
                        "  ELSE 'dup' " +
                        "END"))
                .matches("VALUES CAST('distinct' AS varchar(8))");
        assertThat(assertions.query(
                "SELECT CASE " +
                        "  WHEN UNIQUE (VALUES 1, 1) THEN 'distinct' " +
                        "  ELSE 'dup' " +
                        "END"))
                .matches("VALUES CAST('dup' AS varchar(8))");
    }

    // TODO: correlated UNIQUE is not yet supported — the inner shape
    // (`EXISTS over GROUP BY HAVING COUNT(*) > 1`) is not one the existing
    // decorrelation rules can lift. The non-correlated cases above cover
    // the common usage; correlated UNIQUE needs new decorrelation work.
}
