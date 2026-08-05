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
public class TestMatchPredicate
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testSimpleMatchHit()
    {
        assertThat(assertions.query("SELECT ROW(1, 'a') MATCH (VALUES (1, 'a'), (2, 'b'))"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT ROW(1, 'a') MATCH SIMPLE (VALUES (1, 'a'))"))
                .matches("VALUES true");
    }

    @Test
    public void testSimpleMatchMiss()
    {
        assertThat(assertions.query("SELECT ROW(99, 'z') MATCH (VALUES (1, 'a'), (2, 'b'))"))
                .matches("VALUES false");
    }

    @Test
    public void testSimpleMatchNullValueWildcard()
    {
        // SIMPLE: a NULL anywhere in R makes the result TRUE
        assertThat(assertions.query("SELECT ROW(CAST(NULL AS INTEGER), 'a') MATCH (VALUES (1, 'a'))"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT ROW(CAST(NULL AS INTEGER), 'a') MATCH (VALUES (1, 'z'))"))
                .matches("VALUES true");
    }

    @Test
    public void testPartialMatchHitWithoutNulls()
    {
        assertThat(assertions.query("SELECT ROW(1, 'a') MATCH PARTIAL (VALUES (1, 'a'), (2, 'b'))"))
                .matches("VALUES true");
    }

    @Test
    public void testPartialMatchNullIsWildcardOnRow()
    {
        // PARTIAL: NULL in R acts as wildcard — matches any S value in that position
        assertThat(assertions.query("SELECT ROW(CAST(NULL AS INTEGER), 'a') MATCH PARTIAL (VALUES (1, 'a'))"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT ROW(CAST(NULL AS INTEGER), 'a') MATCH PARTIAL (VALUES (1, 'z'))"))
                .matches("VALUES false");
    }

    @Test
    public void testPartialMatchAllNullsInRow()
    {
        // PARTIAL with all-NULL R → TRUE regardless of subquery (SQL:2003 §8.5 case b.i)
        assertThat(assertions.query("SELECT ROW(CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR(1))) MATCH PARTIAL (VALUES (1, 'a'))"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT ROW(CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR(1))) MATCH PARTIAL (SELECT 1, 'a' WHERE false)"))
                .matches("VALUES true");
    }

    @Test
    public void testFullMatchHit()
    {
        assertThat(assertions.query("SELECT ROW(1, 'a') MATCH FULL (VALUES (1, 'a'), (2, 'b'))"))
                .matches("VALUES true");
    }

    @Test
    public void testFullMatchMixedNullsIsFalse()
    {
        // FULL: NULL in R is OK only if all are NULL; mixed null/non-null is FALSE
        assertThat(assertions.query("SELECT ROW(CAST(NULL AS INTEGER), 'a') MATCH FULL (VALUES (1, 'a'))"))
                .matches("VALUES false");
    }

    @Test
    public void testFullMatchAllNullsIsTrue()
    {
        // FULL: all NULLs in R → TRUE regardless of subquery
        assertThat(assertions.query("SELECT ROW(CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR(1))) MATCH FULL (VALUES (1, 'a'))"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT ROW(CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR(1))) MATCH FULL (SELECT 1, 'a' WHERE false)"))
                .matches("VALUES true");
    }

    @Test
    public void testFullMatchNoMatchInSubquery()
    {
        assertThat(assertions.query("SELECT ROW(99, 'z') MATCH FULL (VALUES (1, 'a'), (2, 'b'))"))
                .matches("VALUES false");
    }

    @Test
    public void testMatchInWhereClause()
    {
        assertThat(assertions.query(
                "SELECT a FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(a, b) " +
                        "WHERE ROW(a, b) MATCH (VALUES (1, 'a'), (3, 'c'))"))
                .matches("VALUES 1, 3");
    }

    @Test
    public void testCorrelatedMatch()
    {
        assertThat(assertions.query(
                "SELECT a FROM (VALUES 1, 2, 3) t(a) " +
                        "WHERE ROW(a) MATCH (SELECT b FROM (VALUES 1, 3, 5) u(b))"))
                .matches("VALUES 1, 3");
    }

    @Test
    public void testSimpleUniqueExactlyOne()
    {
        assertThat(assertions.query("SELECT ROW(1, 'a') MATCH UNIQUE (VALUES (1, 'a'))"))
                .matches("VALUES true");
        // duplicate match → not unique → false
        assertThat(assertions.query("SELECT ROW(1, 'a') MATCH UNIQUE (VALUES (1, 'a'), (1, 'a'))"))
                .matches("VALUES false");
        // no match → false
        assertThat(assertions.query("SELECT ROW(99, 'z') MATCH UNIQUE (VALUES (1, 'a'))"))
                .matches("VALUES false");
    }

    @Test
    public void testSimpleUniqueWithNullInRow()
    {
        // SIMPLE UNIQUE: NULL in R short-circuits to TRUE
        assertThat(assertions.query("SELECT ROW(CAST(NULL AS INTEGER), 'a') MATCH UNIQUE (VALUES (1, 'a'), (2, 'b'))"))
                .matches("VALUES true");
    }

    @Test
    public void testPartialUniqueWithWildcard()
    {
        // PARTIAL UNIQUE: NULL is wildcard; exactly one row matches at non-null positions
        assertThat(assertions.query("SELECT ROW(CAST(NULL AS INTEGER), 'a') MATCH UNIQUE PARTIAL (VALUES (1, 'a'), (2, 'b'))"))
                .matches("VALUES true");
        // wildcard matches two rows → not unique → false
        assertThat(assertions.query("SELECT ROW(CAST(NULL AS INTEGER), 'a') MATCH UNIQUE PARTIAL (VALUES (1, 'a'), (2, 'a'))"))
                .matches("VALUES false");
    }

    @Test
    public void testPartialUniqueAllNullsInRow()
    {
        // PARTIAL with all-NULL R → TRUE regardless of subquery size (SQL:2003 §8.5 case b.i)
        assertThat(assertions.query("SELECT ROW(CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR(1))) MATCH UNIQUE PARTIAL (VALUES (1, 'a'), (2, 'b'))"))
                .matches("VALUES true");
    }

    @Test
    public void testFullUniqueExactlyOne()
    {
        assertThat(assertions.query("SELECT ROW(1, 'a') MATCH UNIQUE FULL (VALUES (1, 'a'), (2, 'b'))"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT ROW(1, 'a') MATCH UNIQUE FULL (VALUES (1, 'a'), (1, 'a'))"))
                .matches("VALUES false");
    }

    @Test
    public void testFullUniqueAllNullsIsTrue()
    {
        assertThat(assertions.query("SELECT ROW(CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR(1))) MATCH UNIQUE FULL (VALUES (1, 'a'))"))
                .matches("VALUES true");
    }

    @Test
    public void testFullUniqueMixedNullsIsFalse()
    {
        assertThat(assertions.query("SELECT ROW(CAST(NULL AS INTEGER), 'a') MATCH UNIQUE FULL (VALUES (1, 'a'))"))
                .matches("VALUES false");
    }

    @Test
    public void testMatchInExtendedCaseWhen()
    {
        // SQL:2023 F262 extended CASE WHEN with a MATCH predicate fragment. The case operand
        // (the ROW value) is the implicit LHS of the MATCH.
        assertThat(assertions.query(
                "SELECT CASE ROW(a, b) " +
                        "  WHEN MATCH (VALUES (1, 'a')) THEN 'first ' " +
                        "  WHEN MATCH (VALUES (2, 'b')) THEN 'second' " +
                        "  ELSE 'none  ' " +
                        "END " +
                        "FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(a, b)"))
                .matches("VALUES CAST('first ' AS varchar(6)), CAST('second' AS varchar(6)), CAST('none  ' AS varchar(6))");
    }
}
