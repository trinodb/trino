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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * End-to-end execution tests for {@code WHEN NOT MATCHED BY SOURCE} in MERGE.
 *
 * <p>Target table {@code mock.default.target} has five rows with keys 1..5 and
 * values 10..50.  The source is always an inline {@code VALUES} subquery so that
 * we can precisely control which rows match, which rows are target-only (→ BY
 * SOURCE), and which rows are source-only (→ BY TARGET).
 *
 * <p>Because MockConnector's merge sink is a no-op, these tests cannot verify
 * the final table state; instead they verify the count of rows processed by the
 * merge plan, which exercises the join-type selection and the three-way CASE
 * condition logic end-to-end.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestMergeNotMatchedBySource
{
    private static final String MOCK_CATALOG = "mock";
    private static final SchemaTableName TARGET_TABLE = new SchemaTableName("default", "target");

    // target schema: key INTEGER, value INTEGER
    private static final List<ColumnMetadata> TARGET_SCHEMA = ImmutableList.of(
            new ColumnMetadata("key", INTEGER),
            new ColumnMetadata("value", INTEGER));

    // target rows: (1,10), (2,20), (3,30), (4,40), (5,50)
    private static final List<List<?>> TARGET_DATA = ImmutableList.of(
            ImmutableList.of(1, 10),
            ImmutableList.of(2, 20),
            ImmutableList.of(3, 30),
            ImmutableList.of(4, 40),
            ImmutableList.of(5, 50));

    private static final Session SESSION = testSessionBuilder()
            .setCatalog(MOCK_CATALOG)
            .setSchema("default")
            .build();

    private final QueryAssertions assertions;

    public TestMergeNotMatchedBySource()
    {
        QueryRunner runner = new StandaloneQueryRunner(SESSION);
        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(TARGET_TABLE)) {
                        return TARGET_SCHEMA;
                    }
                    throw new UnsupportedOperationException("Unknown table: " + schemaTableName);
                })
                .withData(schemaTableName -> {
                    if (schemaTableName.equals(TARGET_TABLE)) {
                        return TARGET_DATA;
                    }
                    throw new UnsupportedOperationException("Unknown table: " + schemaTableName);
                })
                .build()));
        runner.createCatalog(MOCK_CATALOG, "mock", ImmutableMap.of());
        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testNotMatchedBySourceOnlyDeletesUnmatchedRows()
    {
        // Source contains keys 2 and 4.  Keys 1, 3, 5 have no matching source row
        // → 3 rows deleted by the BY SOURCE clause.
        // Keys 2 and 4 match but no MATCHED clause is present → they are silently ignored.
        assertThat(assertions.query(
                """
                MERGE INTO mock.default.target t
                USING (VALUES 2, 4) s(k) ON t.key = s.k
                WHEN NOT MATCHED BY SOURCE THEN DELETE
                """))
                .matches("SELECT BIGINT '3'");
    }

    @Test
    public void testMatchedAndNotMatchedBySourceCorrectRowCounts()
    {
        // LEFT join: keys 2 and 4 match → 2 UPDATEs; keys 1, 3, 5 have no source → 3 DELETEs.
        assertThat(assertions.query(
                """
                MERGE INTO mock.default.target t
                USING (VALUES 2, 4) s(k) ON t.key = s.k
                WHEN MATCHED THEN UPDATE SET value = t.value * 2
                WHEN NOT MATCHED BY SOURCE THEN DELETE
                """))
                .matches("SELECT BIGINT '5'");
    }

    @Test
    public void testAllThreeClauseKindsCorrectRowCounts()
    {
        // FULL join: key 2 matches → 1 UPDATE; key 6 not in target → 1 INSERT;
        // keys 1, 3, 4, 5 have no matching source row → 4 DELETEs.  Total = 6.
        assertThat(assertions.query(
                """
                MERGE INTO mock.default.target t
                USING (VALUES 2, 6) s(k) ON t.key = s.k
                WHEN MATCHED THEN UPDATE SET value = t.value * 2
                WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.k, 60)
                WHEN NOT MATCHED BY SOURCE THEN DELETE
                """))
                .matches("SELECT BIGINT '6'");
    }

    // ---- predicate pushdown correctness ----

    @Test
    public void testBySourcePredicatePushdownSingleClause()
    {
        // All clauses are BY SOURCE with a predicate → pushdown applies.
        // Rows 3 (value=30) and 5 (value=50) satisfy value > 20 → DELETE.
        // Row 1 (value=10) does not satisfy value > 20 → no action.
        // Rows 2 and 4 match source → no BY SOURCE action (no MATCHED clause → pass-through).
        assertThat(assertions.query(
                """
                MERGE INTO mock.default.target t
                USING (VALUES 2, 4) s(k) ON t.key = s.k
                WHEN NOT MATCHED BY SOURCE AND t.value > 20 THEN DELETE
                """))
                .matches("SELECT BIGINT '2'");
    }

    @Test
    public void testBySourcePredicatePushdownMultipleClauses()
    {
        // All BY SOURCE clauses carry predicates → pushdown fires on their disjunction.
        // Keys 1, 3, 5 have no source match.  First clause (value > 30): key 5 (50) → DELETE.
        // Second clause (value <= 30): keys 1 (10) and 3 (30) → UPDATE.  Total = 3.
        assertThat(assertions.query(
                """
                MERGE INTO mock.default.target t
                USING (VALUES 2, 4) s(k) ON t.key = s.k
                WHEN NOT MATCHED BY SOURCE AND t.value > 30 THEN DELETE
                WHEN NOT MATCHED BY SOURCE AND t.value <= 30 THEN UPDATE SET value = 0
                """))
                .matches("SELECT BIGINT '3'");
    }

    @Test
    public void testBySourceNoPushdownWhenUnconditionalClause()
    {
        // Second BY SOURCE clause is unconditional → no pushdown, result still correct.
        assertThat(assertions.query(
                """
                MERGE INTO mock.default.target t
                USING (VALUES 2, 4) s(k) ON t.key = s.k
                WHEN NOT MATCHED BY SOURCE AND t.value > 20 THEN DELETE
                WHEN NOT MATCHED BY SOURCE THEN UPDATE SET value = 0
                """))
                .matches("SELECT BIGINT '3'");
    }

    @Test
    public void testBySourceNoPushdownWhenMatchedClausePresent()
    {
        // MATCHED clause present → no pushdown.  All matched and unmatched rows handled correctly.
        // Keys 2 and 4 match source → 2 UPDATEs.  Keys 1 (10), 3 (30), 5 (50) have no source.
        // BY SOURCE fires for all three (value > 0 always true) → 3 DELETEs.  Total = 5.
        assertThat(assertions.query(
                """
                MERGE INTO mock.default.target t
                USING (VALUES 2, 4) s(k) ON t.key = s.k
                WHEN MATCHED THEN UPDATE SET value = t.value * 2
                WHEN NOT MATCHED BY SOURCE AND t.value > 0 THEN DELETE
                """))
                .matches("SELECT BIGINT '5'");
    }

    @Test
    public void testMultipleBySourceClausesWithPredicates()
    {
        // Source contains keys 2 and 4; keys 1, 3, 5 have no matching source row.
        // First BY SOURCE clause fires when value > 20: keys 3 (30) and 5 (50) → 2 DELETEs.
        // Second BY SOURCE clause is the catch-all: key 1 (10) → 1 UPDATE.
        // Total = 3.
        assertThat(assertions.query(
                """
                MERGE INTO mock.default.target t
                USING (VALUES 2, 4) s(k) ON t.key = s.k
                WHEN NOT MATCHED BY SOURCE AND t.value > 20 THEN DELETE
                WHEN NOT MATCHED BY SOURCE THEN UPDATE SET value = 0
                """))
                .matches("SELECT BIGINT '3'");
    }
}
