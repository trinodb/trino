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

import static io.trino.spi.StandardErrorCode.DUPLICATE_COLUMN_NAME;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.MISSING_COLUMN_ALIASES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestPivot
{
    private static final String SALES = """
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
        assertThat(assertions.query("""
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 AS jan, 2 AS feb, 3 AS mar))
                """.formatted(SALES)))
                .matches("VALUES (BIGINT '190', BIGINT '70', BIGINT '20')");
    }

    @Test
    public void testSingleAggregationWithGroupBy()
    {
        assertThat(assertions.query("""
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 AS jan, 2 AS feb) GROUP BY region)
                ORDER BY region
                """.formatted(SALES)))
                .ordered()
                .matches("""
                        VALUES
                            ('EU', BIGINT '40', CAST(NULL AS BIGINT)),
                            ('NA', BIGINT '150', BIGINT '70')
                        """);
    }

    @Test
    public void testMultipleAggregations()
    {
        assertThat(assertions.query("""
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
                .matches("""
                        VALUES
                            ('EU', BIGINT '40', BIGINT '1', CAST(NULL AS BIGINT), BIGINT '0'),
                            ('NA', BIGINT '150', BIGINT '2', BIGINT '70', BIGINT '1')
                        """);
    }

    @Test
    public void testMultiplePivotColumns()
    {
        assertThat(assertions.query("""
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
        // Aggregation slot is a general expression containing aggregates;
        // FILTER is attached to each aggregate inside.
        assertThat(assertions.query("""
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
                .matches("""
                        VALUES
                            ('EU', BIGINT '0'),
                            ('NA', BIGINT '50')
                        """);
    }

    @Test
    public void testValueWithoutAlias()
    {
        // Unaliased values use literal SQL text as the column name, so 1 and '1'
        // produce distinct columns (both nominally compare to month though only one
        // matches month's bigint type).
        assertThat(assertions.query("""
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
        assertThat(assertions.query("""
                SELECT cnt_null_cnt
                FROM %s
                PIVOT (count(amount) AS cnt FOR month IN (NULL AS cnt_null))
                """.formatted(SALES)))
                .matches("VALUES BIGINT '0'");
    }

    @Test
    public void testRelationAlias()
    {
        assertThat(assertions.query("""
                SELECT p.r, p.jan
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 AS jan) GROUP BY region) AS p (r, jan)
                ORDER BY p.r
                """.formatted(SALES)))
                .ordered()
                .matches("""
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
        assertThat(assertions.query("""
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
        assertThat(assertions.query("""
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 AS jan))
                """.formatted(SALES)))
                .matches("VALUES BIGINT '190'");
    }

    @Test
    public void testMultiplePivotColumnsTupleArityMismatch()
    {
        assertThat(assertions.query("""
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
        assertThat(assertions.query("""
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
        assertThat(assertions.query("""
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 AS jan, 2 AS jan))
                """.formatted(SALES)))
                .failure()
                .hasErrorCode(DUPLICATE_COLUMN_NAME)
                .hasMessageContaining("PIVOT produces duplicate output column name");
    }

    @Test
    public void testGroupingSetsInsidePivot()
    {
        // GROUP BY GROUPING SETS within PIVOT — emits a row per grouping set, with NULL
        // for missing dimensions.
        assertThat(assertions.query("""
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
                .matches("""
                        VALUES
                            (CAST(NULL AS varchar(2)), BIGINT '190'),
                            ('EU', BIGINT '40'),
                            ('NA', BIGINT '150')
                        """);
    }

    @Test
    public void testValueIsExpression()
    {
        // IN values can be arbitrary constant expressions; coercion follows the
        // pivot column's type.
        assertThat(assertions.query("""
                SELECT *
                FROM %s
                PIVOT (sum(amount) FOR month IN (1 + 0 AS jan, 2 * 1 AS feb))
                """.formatted(SALES)))
                .matches("VALUES (BIGINT '190', BIGINT '70')");
    }

    @Test
    public void testAggregationSlotMustBeAggregating()
    {
        // No aggregate function in the slot — caught by AggregationAnalyzer in the
        // rewritten query.
        assertThat(assertions.query("""
                SELECT *
                FROM %s
                PIVOT (amount FOR month IN (1 AS jan) GROUP BY region)
                """.formatted(SALES)))
                .failure()
                .hasMessageContaining("must be an aggregate expression or appear in GROUP BY clause");
    }
}
