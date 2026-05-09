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

import io.trino.Session;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_AGGREGATE;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_CONSTANT;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_SCALAR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.MISSING_COLUMN_ALIASES;
import static io.trino.spi.StandardErrorCode.NESTED_WINDOW;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestPivot
{
    private static final String SALES =
            """
            (VALUES
              ('NA', 1, BIGINT '100'),
              ('NA', 1, BIGINT '50'),
              ('NA', 2, BIGINT '70'),
              ('EU', 1, BIGINT '40'),
              ('EU', 3, BIGINT '20')
            ) AS sales(region, month, amount)
            """;

    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testSingleAggregationNoGroupBy()
    {
        // No implicit grouping: collapses to a single row.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 AS jan, 2 AS feb, 3 AS mar))
                """.formatted(SALES)))
                .matches("VALUES (BIGINT '190', BIGINT '70', BIGINT '20')");
    }

    @Test
    public void testSingleAggregationWithGroupBy()
    {
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 AS jan, 2 AS feb) GROUP BY region)
                ORDER BY region
                """.formatted(SALES)))
                .ordered()
                .matches(
                        """
                        VALUES
                            ('EU', BIGINT '40', CAST(NULL AS BIGINT)),
                            ('NA', BIGINT '150', BIGINT '70')
                        """);
    }

    @Test
    public void testMultipleAggregations()
    {
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (
                    sum(amount) AS total,
                    count(amount) AS cnt
                    FOR month IN (1 AS jan, 2 AS feb)
                    GROUP BY region
                )
                ORDER BY region
                """.formatted(SALES)))
                .ordered()
                .matches(
                        """
                        VALUES
                            ('EU', BIGINT '40', BIGINT '1', CAST(NULL AS BIGINT), BIGINT '0'),
                            ('NA', BIGINT '150', BIGINT '2', BIGINT '70', BIGINT '1')
                        """);
    }

    @Test
    public void testMultiplePivotColumns()
    {
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (
                    sum(amount)
                    FOR (region, month) IN (
                        ('NA', 1) AS na_jan,
                        ('NA', 2) AS na_feb,
                        ('EU', 1) AS eu_jan
                    )
                )
                """.formatted(SALES)))
                .matches("VALUES (BIGINT '150', BIGINT '70', BIGINT '40')");
    }

    @Test
    public void testAggregationExpression()
    {
        // Aggregation slot is a general expression containing multiple aggregates.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (
                    sum(amount) - max(amount) AS minor
                    FOR month IN (1 AS jan)
                    GROUP BY region
                )
                ORDER BY region
                """.formatted(SALES)))
                .ordered()
                .matches(
                        """
                        VALUES
                            ('EU', BIGINT '0'),
                            ('NA', BIGINT '50')
                        """);
    }

    @Test
    public void testSlotSubqueryInsideAggregate()
    {
        // A subquery anywhere inside an aggregate call — its arguments, FILTER, or ORDER BY — is
        // planned before the aggregation, with the other inputs.

        // In an argument: returns each row's amount, so the sum for month 1 is 190.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (sum((SELECT amount)) AS total FOR month IN (1 AS jan))
                """.formatted(SALES)))
                .matches("VALUES BIGINT '190'");

        // In a FILTER: EXISTS (SELECT 1) is always true, so every row for month 1 is summed.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (sum(amount) FILTER (WHERE EXISTS (SELECT 1)) AS total FOR month IN (1 AS jan))
                """.formatted(SALES)))
                .matches("VALUES BIGINT '190'");

        // In an ORDER BY sort key: orders EU's amounts (40, 20) by (SELECT amount) ascending.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (array_agg(amount ORDER BY (SELECT amount)) AS amounts FOR region IN ('EU' AS eu))
                """.formatted(SALES)))
                .matches("VALUES ARRAY[BIGINT '20', BIGINT '40']");
    }

    @Test
    public void testSlotRejectsSubqueryOutsideAggregate()
    {
        // A subquery outside the aggregate calls would have to be planned per value group, which
        // PIVOT does not support yet. This covers the scalar, EXISTS, and IN forms.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT ((sum(amount) + (SELECT BIGINT '1')) AS total FOR month IN (1 AS jan))
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("PIVOT expression cannot contain a subquery outside an aggregate function");

        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT ((sum(amount) + (CASE WHEN EXISTS (SELECT 1) THEN 1 ELSE 0 END)) AS total FOR month IN (1 AS jan))
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("PIVOT expression cannot contain a subquery outside an aggregate function");

        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT ((sum(amount) + (CASE WHEN 1 IN (SELECT 1) THEN 1 ELSE 0 END)) AS total FOR month IN (1 AS jan))
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("PIVOT expression cannot contain a subquery outside an aggregate function");
    }

    @Test
    public void testAggregateFilterInSlot()
    {
        // A FILTER on a pivot-slot aggregate is ANDed into that aggregate's value-group
        // predicate. The unfiltered total alongside it is unaffected: NA's filtered sum is
        // 100 (only amount > 50 qualifies), while its total stays 150.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (
                    sum(amount) AS total,
                    sum(amount) FILTER (WHERE amount > 50) AS big
                    FOR month IN (1 AS jan)
                    GROUP BY region
                )
                ORDER BY region
                """.formatted(SALES)))
                .ordered()
                .matches(
                        """
                        VALUES
                            ('EU', BIGINT '40', CAST(NULL AS BIGINT)),
                            ('NA', BIGINT '150', BIGINT '100')
                        """);
    }

    @Test
    public void testValueWithoutAlias()
    {
        // Unaliased values use literal SQL text as the column name, so 1 and '1'
        // produce distinct columns (both nominally compare to month though only one
        // matches month's bigint type).
        assertThat(assertions.query(
                """
                SELECT "1"
                FROM %s
                PIVOT (sum(amount) FOR month IN (1))
                """.formatted(SALES)))
                .matches("VALUES BIGINT '190'");
    }

    @Test
    public void testNullValue()
    {
        // NULL value uses Trino's standard '=' semantics: never matches, so the
        // synthesized column is the empty-input aggregation result (0 for count).
        // Single-agg with both value-alias and agg-alias produces "valueAlias_aggAlias".
        assertThat(assertions.query(
                """
                SELECT cnt_null_cnt
                FROM %s
                PIVOT (count(amount) AS cnt FOR month IN (NULL AS cnt_null))
                """.formatted(SALES)))
                .matches("VALUES BIGINT '0'");
    }

    @Test
    public void testRelationAlias()
    {
        assertThat(assertions.query(
                """
                SELECT p.r, p.jan
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 AS jan) GROUP BY region) AS p (r, jan)
                ORDER BY p.r
                """.formatted(SALES)))
                .ordered()
                .matches(
                        """
                        VALUES
                            ('EU', BIGINT '40'),
                            ('NA', BIGINT '150')
                        """);
    }

    @Test
    public void testPivotOfPivot()
    {
        // PIVOT output is a relation; another PIVOT can apply to it. The first PIVOT
        // is wrapped in a subquery so the second one can attach.
        assertThat(assertions.query(
                """
                SELECT *
                FROM (
                    SELECT *
                    FROM %s
                    PIVOT (sum(amount) FOR month IN (1 AS jan, 2 AS feb) GROUP BY region)
                ) PIVOT (sum(jan) FOR region IN ('NA' AS na_total))
                """.formatted(SALES)))
                .matches("VALUES (BIGINT '150')");
    }

    @Test
    public void testCoercion()
    {
        // Pivot column is BIGINT; integer literal in IN coerces.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 AS jan))
                """.formatted(SALES)))
                .matches("VALUES BIGINT '190'");
    }

    @Test
    public void testValueWiderThanColumn()
    {
        // The IN value's type need not coerce *to* the pivot column's type; it is enough
        // that the two share a common supertype, matching plain '=' semantics. Here month
        // is INTEGER and the value is BIGINT, so the comparison happens at BIGINT.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (count(amount) AS cnt FOR month IN (1 AS jan, 9999999999 AS huge))
                """.formatted(SALES)))
                .matches("VALUES (BIGINT '3', BIGINT '0')");
    }

    @Test
    public void testValueNotComparableWithColumn()
    {
        // No common supertype between the value and the pivot column: rejected, just as a
        // plain 'month = ...' comparison of the same types would be.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN (VARCHAR 'x'))
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessageContaining("not comparable");
    }

    @Test
    public void testAggregateOrderByInSlot()
    {
        // The aggregate's ORDER BY sort key is projected before pivot coercion, so
        // array_agg(... ORDER BY ...) plans correctly even when the sort key is not
        // one of the aggregate's arguments.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (
                    array_agg(amount ORDER BY month) AS amounts
                    FOR region IN ('EU' AS eu)
                )
                """.formatted(SALES)))
                .matches("VALUES (ARRAY[BIGINT '40', BIGINT '20'])");
    }

    @Test
    public void testInValueRejectsAggregate()
    {
        // An IN value names one output column, so it must be a constant expression; an
        // aggregate, which summarizes rows rather than yielding a fixed value, is rejected.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN (max(month)))
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(EXPRESSION_NOT_SCALAR)
                .hasMessageContaining("PIVOT IN clause cannot contain aggregations");
    }

    @Test
    public void testGroupByAutoRejected()
    {
        // PIVOT has no SELECT list to derive AUTO grouping columns from, so AUTO is
        // explicitly rejected rather than silently degrading to GROUP BY ().
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN (1) GROUP BY AUTO)
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("GROUP BY AUTO is not supported in PIVOT");
    }

    @Test
    public void testMultiplePivotColumnsTupleArityMismatch()
    {
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR (region, month) IN (('NA', 1), ('EU')))
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessageContaining("Number of pivot values");
    }

    @Test
    public void testMultipleAggregationsRequireAlias()
    {
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (sum(amount), count(amount) FOR month IN (1))
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(MISSING_COLUMN_ALIASES)
                .hasMessageContaining("PIVOT with multiple aggregations requires an alias");
    }

    @Test
    public void testDuplicateOutputColumnName()
    {
        // Duplicate output column names are allowed, as in a SELECT list: SELECT * returns both.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 AS jan, 2 AS jan))
                """.formatted(SALES)))
                .matches("VALUES (BIGINT '190', BIGINT '70')");

        // Referencing the ambiguous name, however, fails.
        assertThat(assertions.query(
                """
                SELECT jan
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 AS jan, 2 AS jan))
                """.formatted(SALES)))
                .failure()
                .hasMessageContaining("Column 'jan' is ambiguous");
    }

    @Test
    public void testGroupingSetsInsidePivot()
    {
        // GROUP BY GROUPING SETS within PIVOT — emits a row per grouping set with NULL for
        // the missing dimensions.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (
                    sum(amount) AS total
                    FOR month IN (1 AS jan)
                    GROUP BY GROUPING SETS ((region), ())
                )
                ORDER BY region NULLS FIRST
                """.formatted(SALES)))
                .ordered()
                .matches(
                        """
                        VALUES
                            (CAST(NULL AS varchar(2)), BIGINT '190'),
                            ('EU', BIGINT '40'),
                            ('NA', BIGINT '150')
                        """);
    }

    @Test
    public void testCubeInsidePivot()
    {
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (
                    sum(amount) AS total
                    FOR month IN (1 AS jan)
                    GROUP BY CUBE (region)
                )
                ORDER BY region NULLS FIRST
                """.formatted(SALES)))
                .ordered()
                .matches(
                        """
                        VALUES
                            (CAST(NULL AS varchar(2)), BIGINT '190'),
                            ('EU', BIGINT '40'),
                            ('NA', BIGINT '150')
                        """);
    }

    @Test
    public void testRollupInsidePivot()
    {
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (
                    sum(amount) AS total
                    FOR month IN (1 AS jan)
                    GROUP BY ROLLUP (region)
                )
                ORDER BY region NULLS FIRST
                """.formatted(SALES)))
                .ordered()
                .matches(
                        """
                        VALUES
                            (CAST(NULL AS varchar(2)), BIGINT '190'),
                            ('EU', BIGINT '40'),
                            ('NA', BIGINT '150')
                        """);
    }

    @Test
    public void testEmptyGroupBy()
    {
        // GROUP BY () inside PIVOT — equivalent to no GROUP BY: collapses to a single row.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (
                    sum(amount) AS total
                    FOR month IN (1 AS jan)
                    GROUP BY ()
                )
                """.formatted(SALES)))
                .matches("VALUES (BIGINT '190')");
    }

    @Test
    public void testValueIsExpression()
    {
        // IN values can be arbitrary constant expressions.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 + 0 AS jan, 2 * 1 AS feb))
                """.formatted(SALES)))
                .matches("VALUES (BIGINT '190', BIGINT '70')");
    }

    @Test
    public void testValueIsQueryConstant()
    {
        // The rule is that a value is the same for every input row of the query, not that it is
        // the same across queries: current_date is fixed for the query, so it is a valid value.
        assertThat(assertions.query(
                """
                SELECT *
                FROM (VALUES (current_date, BIGINT '7')) AS t(day, amount)
                PIVOT (sum(amount) FOR day IN (current_date AS today))
                """))
                .matches("VALUES BIGINT '7'");
    }

    @Test
    public void testValueRejectsSubquery()
    {
        // A value is a constant expression in the sense the analyzer uses everywhere else, which
        // excludes a subquery: the query it runs is opaque to the value's own checks, so what the
        // value stands for could not be constrained.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN ((SELECT 1) AS jan))
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(EXPRESSION_NOT_CONSTANT)
                .hasMessageContaining("PIVOT IN clause must be constant and cannot contain a subquery");

        // Including one that reads a WITH query, which is how a non-deterministic value would
        // otherwise sneak in.
        assertThat(assertions.query(
                """
                WITH pivot_value(v) AS (SELECT CAST(random(3) AS integer))
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN ((SELECT v FROM pivot_value) AS lucky))
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(EXPRESSION_NOT_CONSTANT)
                .hasMessageContaining("PIVOT IN clause must be constant and cannot contain a subquery");
    }

    @Test
    public void testValueRejectsColumnReference()
    {
        // An IN value identifies one output column, so it must not vary per input row.
        // Without this, 'month' would match every row against its own value.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN (month AS same))
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(EXPRESSION_NOT_CONSTANT)
                .hasMessageContaining("PIVOT IN clause must be constant and cannot reference a column: month");
    }

    @Test
    public void testValueWithShadowedColumnName()
    {
        // The lambda argument 'month' shadows the pivot column of the same name, so the value is
        // constant despite mentioning that name. It evaluates to 1 and matches month = 1.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN (transform(ARRAY[1], month -> month)[1] AS one))
                """.formatted(SALES)))
                .matches("VALUES BIGINT '190'");
    }

    @Test
    public void testValueRejectsNonDeterministicExpression()
    {
        // A non-deterministic value is evaluated per input row, so it would scatter rows across
        // the output column it is supposed to name.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (count(*) FOR month IN (CAST(random(3) AS integer) AS lucky))
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(EXPRESSION_NOT_CONSTANT)
                .hasMessageContaining("PIVOT IN clause must be constant and cannot be non-deterministic");
    }

    @Test
    public void testAggregationSlotMustContainAggregate()
    {
        // The pivot value scoping applies to the aggregates in the slot, so a slot without
        // one is meaningless: the column reference case would need the pivot filter it never
        // gets, and the constant case would ignore the pivot value entirely.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (amount FOR month IN (1 AS jan) GROUP BY region)
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageContaining("PIVOT expression must contain an aggregate function: amount");

        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (1 FOR month IN (1 AS jan))
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageContaining("PIVOT expression must contain an aggregate function: 1");
    }

    @Test
    public void testAggregationSlotRejectsWindowFunction()
    {
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (sum(sum(amount)) OVER () AS total FOR month IN (1 AS jan) GROUP BY region)
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(NESTED_WINDOW)
                .hasMessageContaining("PIVOT expression cannot contain window functions or row pattern measures");
    }

    @Test
    public void testAggregationSlotRejectsGroupingOperation()
    {
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (
                    sum(amount) + grouping(region) AS total
                    FOR month IN (1 AS jan)
                    GROUP BY ROLLUP (region)
                )
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("PIVOT expression cannot contain grouping operations");
    }

    @Test
    public void testOutputColumnNames()
    {
        // Output columns are, in order: the GROUP BY columns, then one column per (value group,
        // aggregation). A column's name is the value's name (alias, or SQL text of the value)
        // with the aggregation alias appended when the aggregation has one.
        assertThat(assertions.query(
                """
                SELECT region, jan_total, jan_cnt, feb_total, feb_cnt
                FROM %s
                PIVOT (
                    sum(amount) AS total,
                    count(amount) AS cnt
                    FOR month IN (1 AS jan, 2 AS feb)
                    GROUP BY region
                )
                ORDER BY region
                """.formatted(SALES)))
                .ordered()
                .matches(
                        """
                        VALUES
                            ('EU', BIGINT '40', BIGINT '1', CAST(NULL AS BIGINT), BIGINT '0'),
                            ('NA', BIGINT '150', BIGINT '2', BIGINT '70', BIGINT '1')
                        """);

        // Single aggregation without an alias: the column is named for the value alone.
        assertThat(assertions.query(
                """
                SELECT jan
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 AS jan))
                """.formatted(SALES)))
                .matches("VALUES BIGINT '190'");

        // Value without an alias: the column is named for the SQL text of the value.
        assertThat(assertions.query(
                """
                SELECT "1_total"
                FROM %s
                PIVOT (sum(amount) AS total FOR month IN (1))
                """.formatted(SALES)))
                .matches("VALUES BIGINT '190'");
    }

    @Test
    public void testOutputColumnNamesAreCanonicalized()
    {
        // An unquoted alias is canonicalized to lower case, so the output column is "jan_total".
        assertThat(assertions.query(
                """
                SELECT jan_total
                FROM %s
                PIVOT (sum(amount) AS Total FOR month IN (1 AS Jan))
                """.formatted(SALES)))
                .matches("VALUES BIGINT '190'");

        // A quoted alias keeps its case, so the output column is "Jan".
        assertThat(assertions.query(
                """
                SELECT "Jan"
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 AS "Jan"))
                """.formatted(SALES)))
                .matches("VALUES BIGINT '190'");

        // A canonicalized GROUP BY column name behaves the same way.
        assertThat(assertions.query(
                """
                SELECT region
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 AS jan) GROUP BY Region)
                ORDER BY region
                """.formatted(SALES)))
                .ordered()
                .matches("VALUES 'EU', 'NA'");
    }

    @Test
    public void testGroupByOutputColumnNames()
    {
        // A simple GROUP BY expression is named for the column; a complex one has no derived name
        // and is addressed positionally.
        assertThat(assertions.query(
                """
                SELECT region, jan
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 AS jan) GROUP BY region)
                ORDER BY region
                """.formatted(SALES)))
                .ordered()
                .matches("VALUES ('EU', BIGINT '40'), ('NA', BIGINT '150')");

        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 AS jan) GROUP BY region || '!')
                ORDER BY 1
                """.formatted(SALES)))
                .ordered()
                .matches("VALUES (CAST('EU!' AS varchar), BIGINT '40'), (CAST('NA!' AS varchar), BIGINT '150')");
    }

    @Test
    public void testValueAndColumnCoerceToCommonSupertype()
    {
        // Neither side is the common supertype: the INTEGER column and the DECIMAL value are both
        // coerced to decimal, and the comparison runs there.
        assertThat(assertions.query(
                """
                SELECT *
                FROM (VALUES (INTEGER '1', BIGINT '10'), (INTEGER '2', BIGINT '20')) AS t(k, v)
                PIVOT (sum(v) FOR k IN (DECIMAL '1' AS one, DECIMAL '2' AS two))
                """))
                .matches("VALUES (BIGINT '10', BIGINT '20')");
    }

    @Test
    public void testColumnTakesDifferentCoercionsPerValueGroup()
    {
        // The same pivot column is compared at a different supertype in each value group: at BIGINT
        // against the BIGINT value, and at INTEGER against the SMALLINT value.
        assertThat(assertions.query(
                """
                SELECT *
                FROM (VALUES (INTEGER '1', BIGINT '10'), (INTEGER '2', BIGINT '20')) AS t(k, v)
                PIVOT (sum(v) FOR k IN (BIGINT '1' AS a, SMALLINT '2' AS b))
                """))
                .matches("VALUES (BIGINT '10', BIGINT '20')");
    }

    @Test
    public void testDistinctAggregation()
    {
        // A DISTINCT aggregate in a slot is planned like any other aggregate. For month 1 the
        // regions are NA, NA, EU, so two distinct values.
        assertThat(assertions.query(
                """
                SELECT *
                FROM %s
                PIVOT (count(DISTINCT region) AS regions FOR month IN (1 AS jan))
                """.formatted(SALES)))
                .matches("VALUES BIGINT '2'");
    }

    @Test
    public void testValueIsParameter()
    {
        // A parameter is constant for the execution, so it is a valid value.
        Session session = Session.builder(assertions.getDefaultSession())
                .addPreparedStatement(
                        "my_query",
                        """
                        SELECT *
                        FROM %s
                        PIVOT (sum(amount) FOR month IN (? AS chosen))
                        """.formatted(SALES))
                .build();
        assertThat(assertions.query(session, "EXECUTE my_query USING 1"))
                .matches("VALUES BIGINT '190'");
    }

    @Test
    public void testValueRejectsCorrelatedColumn()
    {
        // A value cannot reference a column of an enclosing query any more than one of the pivot
        // input: it must be constant.
        assertThat(assertions.query(
                """
                SELECT (
                    SELECT total
                    FROM %s
                    PIVOT (sum(amount) AS total FOR month IN (o.month))
                )
                FROM (VALUES 1) AS o(month)
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(EXPRESSION_NOT_CONSTANT)
                .hasMessageContaining("PIVOT IN clause must be constant");
    }
}
