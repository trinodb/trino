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
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.SystemSessionProperties.USE_LEGACY_DECORRELATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * End-to-end tests for the unified dependent-join framework (Neumann &amp; Kemper, BTW 2015).
 * <p>
 * All queries here run with the dependent-join framework (`use_legacy_decorrelator = false`). The framework should produce
 * the same answers as the legacy decorrelator on shapes the legacy decorrelator handles, and
 * should additionally handle inequality-correlated subqueries (the canonical case the legacy
 * `Reference = Reference` mapping rejects).
 */
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDecorrelateDependentJoin
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    private Session frameworkSession()
    {
        return assertions.sessionBuilder()
                .setSystemProperty(USE_LEGACY_DECORRELATOR, "false")
                .build();
    }

    private Session legacySession()
    {
        return assertions.sessionBuilder()
                .setSystemProperty(USE_LEGACY_DECORRELATOR, "true")
                .build();
    }

    @Test
    public void testScalarSubqueryReturningBareCorrelationFailsAsUnsupported()
    {
        // A scalar subquery whose output is a bare correlation symbol (`SELECT t.a …`) aliases the inner
        // correlation to the outer symbol; decorrelating it would return the outer value for every row
        // (e.g. [1, 1] here) instead of NULL for the non-matching row. The framework must decline; the shape fails as
        // unsupported (legacy also rejects it). Regression test for the bare-correlation-scalar-output bug.
        assertThat(assertions.query(
                frameworkSession(),
                "SELECT (SELECT t.a WHERE t.a = 1) FROM (VALUES 1, 2) t(a)"))
                .failure().hasMessageContaining("Given correlated subquery is not supported");
    }

    @Test
    public void testQuantifiedComparisonOverCorrelationFailsAsUnsupported()
    {
        // A correlated quantified comparison `x > ALL (VALUES k)` plans as a scalar global aggregate
        // whose base (the VALUES row) embeds the correlation, i.e. the aggregate runs over the
        // correlation value itself. Decorrelating it produces a malformed distinct aggregation that a
        // later optimizer rule rejects, so the framework must decline; the shape fails as unsupported (legacy also rejects it).
        assertThat(assertions.query(
                frameworkSession(),
                "SELECT 1 > ALL (VALUES k) FROM (VALUES (1, 1), (1, 2)) t(k, v) GROUP BY k"))
                .failure().hasMessageContaining("Given correlated subquery is not supported");
    }

    @Test
    public void testCorrelationCarriedAsStructuralOutputFailsAsUnsupported()
    {
        // A correlation symbol carried as a structural output (the derived relation `(SELECT l1.a)`
        // selects the outer column directly) can't be rebound to the cloned correlation. The
        // framework must decline rather than emit a malformed join; legacy also rejects this shape, so
        // the result is the clean "not supported" error (regression test for the join-output bug).
        assertThat(assertions.query(
                frameworkSession(),
                "SELECT (SELECT l3.x FROM (VALUES 10, 20) l2(y) CROSS JOIN (SELECT l1.a) l3(x) LIMIT 1) FROM (VALUES 1, 2) l1(a)"))
                .failure().hasMessageContaining("Given correlated subquery is not supported");
    }

    @Test
    public void testLeftLateralConstantProjectionNullExtends()
    {
        // LEFT JOIN LATERAL (SELECT <constant> WHERE p(corr)): for outer rows where the correlated
        // filter excludes every subquery row, the join null-extends, so the projected column must be
        // NULL — not the constant. A naive plain-LEFT-join lifts the constant projection above the
        // join and wrongly keeps it for unmatched rows; the framework must null-extend correctly
        // (regression test for the lifted-projection-over-LEFT-join bug). a=1: a=2 false → NULL.
        assertThat(assertions.query(
                frameworkSession(),
                "SELECT b FROM (VALUES 1) t(a) LEFT JOIN LATERAL (SELECT 2 WHERE a = 2) t2(b) ON true"))
                .matches("VALUES CAST(null AS INTEGER)");
    }

    @Test
    public void testCorrelatedFilterEquality()
    {
        // σ pushdown over a Filter + TableScan-like (Values) — the canonical SPJ case.
        assertThat(assertions.query(frameworkSession(),
                "SELECT a FROM (VALUES 1, 2, 3) t(a) " +
                        "WHERE EXISTS (SELECT 1 FROM (VALUES 1, 3, 5) u(b) WHERE u.b = t.a)"))
                .matches("VALUES 1, 3");
    }

    @Test
    public void testSingleSideCorrelatedInnerJoinInLateral()
    {
        // Inner join inside a LATERAL where only the LEFT side carries correlation (in a projection,
        // which legacy can't decorrelate) and the right side is correlation-free. This hits
        // pushInnerJoin's single-side branch → rebuildJoin → chooseOutputs, which must surface T₁'s
        // input column (t.a) so the rebuilt join still produces it for the outer projection.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, x.s_p, x.w_k " +
                        "FROM (VALUES 1, 2) t(a) " +
                        "CROSS JOIN LATERAL (" +
                        "  SELECT s.p AS s_p, w.k AS w_k " +
                        "  FROM (SELECT t.a * 10 + u.v AS p FROM (VALUES 1, 2) u(v)) s " +
                        "  JOIN (VALUES 11, 12, 21, 22) w(k) ON s.p = w.k" +
                        ") x"))
                .matches("VALUES (1, 11, 11), (1, 12, 12), (2, 21, 21), (2, 22, 22)");
    }

    @Test
    public void testNotExists()
    {
        // NOT EXISTS is also a LEFT-derived correlated join; the framework's LEFT path must
        // produce correct anti-correlation results.
        assertThat(assertions.query(frameworkSession(),
                "SELECT a FROM (VALUES 1, 2, 3) t(a) " +
                        "WHERE NOT EXISTS (SELECT 1 FROM (VALUES 1, 3, 5) u(b) WHERE u.b = t.a)"))
                .matches("VALUES 2");
    }

    @Test
    public void testNonAggregateScalarSubqueryWithCorrelationInProjection()
    {
        // Non-aggregate scalar subquery (EnforceSingleRowNode) with correlation in the projection
        // (`t.a + u.b`) — legacy can't decorrelate correlation in a projection; the framework
        // does. ≤1 match per outer; a=3 has no match → NULL.
        assertThat(assertions.query(frameworkSession(),
                "SELECT a, (SELECT t.a + u.b FROM (VALUES (1, 10), (2, 20)) u(k, b) WHERE u.k = t.a) " +
                        "FROM (VALUES 1, 2, 3) t(a)"))
                .matches("VALUES (1, 11), (2, 22), (3, CAST(NULL AS INTEGER))");
    }

    @Test
    public void testScalarSubqueryMultipleRowsFails()
    {
        // A non-aggregate scalar subquery that returns >1 row for some outer row must error, just
        // as it does under legacy — the per-correlation-group row_number + fail check enforces it.
        assertThat(assertions.query(frameworkSession(),
                "SELECT a, (SELECT u.b FROM (VALUES (1, 10), (1, 20)) u(k, b) WHERE u.k = t.a) " +
                        "FROM (VALUES 1) t(a)"))
                .failure().hasMessageContaining("Scalar sub-query has returned multiple rows");
    }

    @Test
    public void testCorrelatedFilterInequality()
    {
        // Inequality correlation — `u.b < t.a`. The unified framework lifts it as a theta-join
        // filter via σ pushdown. The legacy decorrelator also handles this shape (via the
        // `TransformCorrelatedJoinToJoin` residual-predicate path), so both modes return the same
        // result — this test is here to confirm the unified path doesn't break it.
        assertThat(assertions.query(frameworkSession(),
                "SELECT a FROM (VALUES 1, 5, 10) t(a) " +
                        "WHERE EXISTS (SELECT 1 FROM (VALUES 3, 7) u(b) WHERE u.b < t.a)"))
                .matches("VALUES 5, 10");
    }

    @Test
    public void testCorrelatedProjection()
    {
        // π pushdown — correlation reference inside a projection (here, `t.a` reused in select list).
        assertThat(assertions.query(frameworkSession(),
                "SELECT a FROM (VALUES 1, 2) t(a) " +
                        "WHERE EXISTS (SELECT t.a + b FROM (VALUES 10, 20) u(b))"))
                .matches("VALUES 1, 2");
    }

    @Test
    public void testCorrelatedGroupedAggregationInsideExists()
    {
        // Γ pushdown — a GROUPED aggregation (GROUP BY u.b) inside EXISTS. The framework adds the
        // outer row's symbols to the grouping keys so the aggregate runs once per outer row.
        // EXISTS maps "no group" → FALSE, which is correct. (A GLOBAL aggregation in value
        // position needs the empty group restored instead — that shape is owned by
        // DecorrelateScalarAggregationViaDependentJoin, which masks with non_null.)
        assertThat(assertions.query(frameworkSession(),
                "SELECT a FROM (VALUES 1, 5, 10) t(a) " +
                        "WHERE EXISTS (SELECT u.b FROM (VALUES 2, 4) u(b) WHERE u.b < t.a GROUP BY u.b)"))
                .matches("VALUES 5, 10");
    }

    @Test
    public void testCorrelatedInnerJoinSingleSide()
    {
        // ⋈ pushdown — inner join where only one side references correlation.
        assertThat(assertions.query(frameworkSession(),
                "SELECT a FROM (VALUES 1, 2, 3) t(a) " +
                        "WHERE EXISTS (SELECT 1 FROM (VALUES 1, 3) u(b) JOIN (VALUES 10, 30) v(c) " +
                        "              ON u.b * 10 = v.c WHERE u.b = t.a)"))
                .matches("VALUES 1, 3");
    }

    @Test
    public void testCorrelatedLimitInLateral()
    {
        // lim pushdown — LATERAL is an INNER correlated join, and LIMIT n bounds the rows per
        // outer row. Counting the lateral rows is deterministic (independent of which n rows
        // LIMIT keeps): for each outer row there are ≥ 2 matches, so the count is capped at 2.
        assertThat(assertions.query(frameworkSession(),
                "SELECT a, count(*) FROM (VALUES 1, 2) t(a), " +
                        "  LATERAL (SELECT u.b FROM (VALUES 1, 2, 3, 4, 5) u(b) WHERE u.b > t.a LIMIT 2) l(b) " +
                        "GROUP BY a"))
                .matches("VALUES (1, BIGINT '2'), (2, BIGINT '2')");
    }

    @Test
    public void testCorrelatedInnerJoinBothSides()
    {
        // ⋈ pushdown — inner join where BOTH sides reference correlation. Reduces to
        // σ_q((T₁ ⋈^D L) ⋈^D R). For each outer row a, the subquery joins u (u.b = a) with
        // v (v.c = a) on u.b = v.c — non-empty only when a appears in both.
        assertThat(assertions.query(frameworkSession(),
                "SELECT a FROM (VALUES 1, 2, 3) t(a) " +
                        "WHERE EXISTS (" +
                        "  SELECT 1 FROM (VALUES 1, 2) u(b) JOIN (VALUES 2, 3) v(c) ON u.b = v.c " +
                        "  WHERE u.b = t.a AND v.c = t.a)"))
                .matches("VALUES 2");
    }

    @Test
    public void testScalarAggregateWithCorrelationInArgument()
    {
        // Scalar global aggregate with correlation in the aggregate ARGUMENT (`sum(t.a + u.b)`).
        // Legacy can't decorrelate correlation in a projection feeding an aggregate; the framework
        // handles it via the source pushdown + non_null-mask re-aggregation.
        //   a=1: u.b>5 → {10,20}, sum(1+10,1+20)=32; a=2: {20}, sum(22)=22; a=3: {20}, sum(23)=23
        assertThat(assertions.query(frameworkSession(),
                "SELECT a, (SELECT sum(t.a + u.b) FROM (VALUES 10, 20) u(b) WHERE u.b > t.a * 5) " +
                        "FROM (VALUES 1, 2, 3) t(a)"))
                .matches("VALUES (1, BIGINT '32'), (2, BIGINT '22'), (3, BIGINT '23')");
    }

    @Test
    public void testScalarCountWithCorrelationInArgument()
    {
        // count over a correlated projection; empty groups must yield 0 (non_null mask), not NULL.
        //   a=1: u.b>5 → {10,20} → count 2; a=100: u.b>500 → {} → count 0
        assertThat(assertions.query(frameworkSession(),
                "SELECT a, (SELECT count(t.a + u.b) FROM (VALUES 10, 20) u(b) WHERE u.b > t.a * 5) " +
                        "FROM (VALUES 1, 100) t(a)"))
                .matches("VALUES (1, BIGINT '2'), (100, BIGINT '0')");
    }

    @Test
    public void testCorrelatedProjectionInLateral()
    {
        // π pushdown under an INNER LATERAL with correlation ONLY in the projection (no filter) —
        // legacy can't decorrelate correlation in a projection; the framework can.
        assertThat(assertions.query(
                frameworkSession(),
                "SELECT t.a, l.x FROM (VALUES 1, 2) t(a), LATERAL (SELECT t.a + u.b AS x FROM (VALUES 10, 20) u(b)) l(x)"))
                .matches("VALUES (1, 11), (1, 21), (2, 12), (2, 22)");
    }

    @Test
    public void testLeftCorrelatedJoinNullExtends()
    {
        // LEFT JOIN LATERAL via the magic-set: every outer row appears; rows whose subquery is
        // empty null-extend. The projection `t.a + u.b` references correlation, so this is beyond
        // legacy (filter-only). a=2 → u.b > 30 is empty → NULL.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.x FROM (VALUES 1, 2) t(a) " +
                        "LEFT JOIN LATERAL (SELECT t.a + u.b AS x FROM (VALUES 10, 20) u(b) WHERE u.b > t.a * 15) l(x) ON true"))
                .matches("VALUES (1, 21), (2, CAST(NULL AS INTEGER))");
    }

    @Test
    public void testLeftCorrelatedJoinMultipleMatches()
    {
        // Each outer row appears once per match; unmatched outer rows null-extend. Exercises the
        // distinct-D + LEFT-join-back with multiplicity. a=9 → no u.b > 9 → NULL.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 1, 9) t(a) " +
                        "LEFT JOIN LATERAL (SELECT u.b FROM (VALUES 2, 3, 4) u(b) WHERE u.b > t.a) l(b) ON true"))
                .matches("VALUES (1, 2), (1, 3), (1, 4), (9, CAST(NULL AS INTEGER))");
    }

    @Test
    public void testLeftCorrelatedJoinDuplicateOuterRows()
    {
        // Duplicate outer correlation values must each get the subquery result (distinct-D folds
        // them, the join-back fans them out). Two a=1 rows both see {2,3}.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 1, 1, 5) t(a) " +
                        "LEFT JOIN LATERAL (SELECT u.b FROM (VALUES 2, 3) u(b) WHERE u.b > t.a) l(b) ON true"))
                .matches("VALUES (1, 2), (1, 3), (1, 2), (1, 3), (5, CAST(NULL AS INTEGER))");
    }

    @Test
    public void testLeftCorrelatedJoinNullCorrelation()
    {
        // NULL correlation value: `u.b > NULL` is UNKNOWN → empty subquery → null-extend. The
        // NULL-safe IS NOT DISTINCT FROM join-back must still produce the NULL outer row exactly
        // once (not drop it, not duplicate it).
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 1, CAST(NULL AS INTEGER)) t(a) " +
                        "LEFT JOIN LATERAL (SELECT u.b FROM (VALUES 2, 3) u(b) WHERE u.b > t.a) l(b) ON true"))
                .matches("VALUES (1, 2), (1, 3), (CAST(NULL AS INTEGER), CAST(NULL AS INTEGER))");
    }

    @Test
    public void testCorrelatedUnionAllInLateral()
    {
        // ⊕ pushdown — UNION ALL of two correlated branches inside an INNER LATERAL. Each branch is
        // decorrelated independently and the results concatenated per outer row; the second branch
        // runs over a fresh clone of the input. Legacy can't decorrelate a correlated set-op.
        //   a=1: u.b>1 → {2,3,4}; v.c<1 → {0}   ⇒ {2,3,4,0}
        //   a=2: u.b>2 → {3,4};   v.c<2 → {0,1} ⇒ {3,4,0,1}
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 1, 2) t(a), LATERAL (" +
                        "  SELECT u.b FROM (VALUES 2, 3, 4) u(b) WHERE u.b > t.a " +
                        "  UNION ALL " +
                        "  SELECT v.c FROM (VALUES 0, 1) v(c) WHERE v.c < t.a) l(b)"))
                .matches("VALUES (1, 2), (1, 3), (1, 4), (1, 0), (2, 3), (2, 4), (2, 0), (2, 1)");
    }

    @Test
    public void testCorrelatedIntersectInLateral()
    {
        // ⊕ — INTERSECT (distinct) evaluates per distinct correlation value (magic-set + INNER
        // join-back): the set-op dedup over a directly distributed bag input would merge duplicate
        // outer rows.
        //   a=1: u.b>=1 → {1,2,3,4} ∩ (v.c<=2 → {2})   = {2}
        //   a=2: u.b>=2 → {2,3,4}   ∩ (v.c<=3 → {2,3}) = {2,3}
        //   a=3: u.b>=3 → {3,4}     ∩ (v.c<=4 → {2,3,4}) = {3,4}
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 1, 2, 3) t(a), LATERAL (" +
                        "  SELECT u.b FROM (VALUES 1, 2, 3, 4) u(b) WHERE u.b >= t.a " +
                        "  INTERSECT " +
                        "  SELECT v.c FROM (VALUES 2, 3, 4, 5) v(c) WHERE v.c <= t.a + 1) l(b)"))
                .matches("VALUES (1, 2), (2, 2), (2, 3), (3, 3), (3, 4)");
    }

    @Test
    public void testCorrelatedExceptInLateral()
    {
        // ⊕ — EXCEPT (distinct), again per distinct correlation value via the magic-set.
        //   a=1: {1,2,3,4} \ (v.c>1 → {2,4}) = {1,3}
        //   a=2: {2,3,4}   \ (v.c>2 → {4})   = {2,3}
        //   a=3: {3,4}     \ (v.c>3 → {4})   = {3}
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 1, 2, 3) t(a), LATERAL (" +
                        "  SELECT u.b FROM (VALUES 1, 2, 3, 4) u(b) WHERE u.b >= t.a " +
                        "  EXCEPT " +
                        "  SELECT v.c FROM (VALUES 2, 4) v(c) WHERE v.c > t.a) l(b)"))
                .matches("VALUES (1, 1), (1, 3), (2, 2), (2, 3), (3, 3)");
    }

    @Test
    public void testDuplicateOuterRowsGroupedAggregationInLateral()
    {
        // Γ pushdown over a bag input — each duplicate outer row keeps its own groups and counts
        // (the unique input id in the grouping keys keeps the copies apart; grouping by the input
        // columns alone would pool both copies into one group with count 4).
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b, l.c FROM (VALUES 1, 1) t(a), LATERAL (" +
                        "  SELECT u.b, count(*) AS c FROM (VALUES (1, 10), (1, 20), (2, 30)) u(b, x) " +
                        "  WHERE u.b = t.a GROUP BY u.b) l(b, c)"))
                .matches("VALUES (1, 1, BIGINT '2'), (1, 1, BIGINT '2')");
    }

    @Test
    public void testDuplicateOuterRowsGroupingSetsInLateral()
    {
        // GROUPING SETS per duplicated outer row — every grouping set is computed once per copy.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.k, l.cnt FROM (VALUES 1, 1) t(a), LATERAL (" +
                        "  SELECT u.k AS k, count(*) AS cnt FROM (VALUES (1, 10), (1, 20)) u(k, b) " +
                        "  WHERE u.k = t.a GROUP BY GROUPING SETS ((u.k), (u.b))) l(k, cnt)"))
                .matches("VALUES (1, 1, BIGINT '2'), (1, CAST(NULL AS INTEGER), BIGINT '1'), " +
                        "(1, CAST(NULL AS INTEGER), BIGINT '1'), " +
                        "(1, 1, BIGINT '2'), (1, CAST(NULL AS INTEGER), BIGINT '1'), " +
                        "(1, CAST(NULL AS INTEGER), BIGINT '1')");
    }

    @Test
    public void testDuplicateOuterRowsUnionAllInLateral()
    {
        // UNION ALL distributes directly even over a bag input — concatenation needs no
        // cross-branch row matching, and each duplicate outer row keeps its own branch rows.
        //   a=1: u.b>1 → {2,3}; v.c<1 → {0} ⇒ {2,3,0}, twice
        //   a=2: u.b>2 → {3};   v.c<2 → {0,1} ⇒ {3,0,1}
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 1, 1, 2) t(a), LATERAL (" +
                        "  SELECT u.b FROM (VALUES 2, 3) u(b) WHERE u.b > t.a " +
                        "  UNION ALL " +
                        "  SELECT v.c FROM (VALUES 0, 1) v(c) WHERE v.c < t.a) l(b)"))
                .matches("VALUES (1, 2), (1, 3), (1, 0), (1, 2), (1, 3), (1, 0), (2, 3), (2, 0), (2, 1)");
    }

    @Test
    public void testDuplicateOuterRowsUnionDistinctInLateral()
    {
        // UNION (distinct) plans as a dedup aggregation over UNION ALL; the dedup must apply per
        // outer row INSTANCE. The set operation below the aggregation forces the magic-set
        // (per-distinct-correlation-value evaluation + join-back), since a unique-id-tagged input
        // would not survive the branch clones.
        //   per copy of a=1: {1,2} ∪ {2,3} = {1,2,3}
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 1, 1) t(a), LATERAL (" +
                        "  SELECT u.b FROM (VALUES 1, 2) u(b) WHERE u.b >= t.a " +
                        "  UNION " +
                        "  SELECT v.c FROM (VALUES 2, 3) v(c) WHERE v.c > t.a) l(b)"))
                .matches("VALUES (1, 1), (1, 2), (1, 3), (1, 1), (1, 2), (1, 3)");
    }

    @Test
    public void testDuplicateOuterRowsIntersectInLateral()
    {
        // INTERSECT (distinct) over a bag input takes the magic-set — distributing the duplicated
        // input directly would merge the copies in the set-op dedup.
        //   per copy of a=2: (u.b>=2 → {2,3,4}) ∩ (v.c<=3 → {2,3}) = {2,3}
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 2, 2) t(a), LATERAL (" +
                        "  SELECT u.b FROM (VALUES 1, 2, 3, 4) u(b) WHERE u.b >= t.a " +
                        "  INTERSECT " +
                        "  SELECT v.c FROM (VALUES 2, 3, 4, 5) v(c) WHERE v.c <= t.a + 1) l(b)"))
                .matches("VALUES (2, 2), (2, 3), (2, 2), (2, 3)");
    }

    @Test
    public void testDuplicateOuterRowsExceptInLateral()
    {
        //   per copy of a=2: (u.b>=2 → {2,3,4}) \ (v.c>2 → {4}) = {2,3}
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 2, 2) t(a), LATERAL (" +
                        "  SELECT u.b FROM (VALUES 2, 3, 4) u(b) WHERE u.b >= t.a " +
                        "  EXCEPT " +
                        "  SELECT v.c FROM (VALUES 2, 4) v(c) WHERE v.c > t.a) l(b)"))
                .matches("VALUES (2, 2), (2, 3), (2, 2), (2, 3)");
    }

    @Test
    public void testDuplicateOuterRowsIntersectAllInLateral()
    {
        // INTERSECT ALL distributes directly over a duplicated input: per-key multiplicities
        // double on both sides, so min(2·m₁, 2·m₂) = 2·min(m₁, m₂) — the per-copy answer, twice.
        //   per copy of a=2: {2,2,3} ∩ALL {2,3,3} = {2,3}
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 2, 2) t(a), LATERAL (" +
                        "  SELECT u.b FROM (VALUES 2, 2, 3) u(b) WHERE u.b >= t.a " +
                        "  INTERSECT ALL " +
                        "  SELECT v.c FROM (VALUES 2, 3, 3) v(c) WHERE v.c >= t.a) l(b)"))
                .matches("VALUES (2, 2), (2, 3), (2, 2), (2, 3)");
    }

    @Test
    public void testDuplicateOuterRowsExceptAllInLateral()
    {
        // EXCEPT ALL distributes directly over a duplicated input: max(2·m₁ − 2·m₂, 0) =
        // 2·max(m₁ − m₂, 0).
        //   per copy of a=2: {2,2,3} \ALL {2,3,3} = {2}
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 2, 2) t(a), LATERAL (" +
                        "  SELECT u.b FROM (VALUES 2, 2, 3) u(b) WHERE u.b >= t.a " +
                        "  EXCEPT ALL " +
                        "  SELECT v.c FROM (VALUES 2, 3, 3) v(c) WHERE v.c >= t.a) l(b)"))
                .matches("VALUES (2, 2), (2, 2)");
    }

    @Test
    public void testCorrelatedUnnest()
    {
        // UNNEST pushdown — the array is a correlation reference (`t.arr`). T₁ is pushed into the
        // unnest source and its columns added to the replicate list, so each outer row is paired
        // with the elements of its own array. (CROSS JOIN UNNEST is an INNER correlated join.)
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, u.x FROM (VALUES (1, ARRAY[10, 20]), (2, ARRAY[30])) t(a, arr) " +
                        "CROSS JOIN UNNEST(arr) u(x)"))
                .matches("VALUES (1, 10), (1, 20), (2, 30)");
    }

    @Test
    public void testScalarAggregateWithCorrelatedFilterMask()
    {
        // count(*) FILTER (WHERE u.b > t.a): the FILTER becomes an aggregate mask referencing
        // correlation. The framework combines it with its own non_null mask. Legacy can't
        // decorrelate a correlated FILTER aggregate, so this is framework-only.
        //   a=1: u.b>1 → {2,3,4} → 3; a=2: {3,4} → 2; a=3: {4} → 1; a=5: {} → 0
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, (SELECT count(*) FILTER (WHERE u.b > t.a) FROM (VALUES 1, 2, 3, 4) u(b)) " +
                        "FROM (VALUES 1, 2, 3, 5) t(a)"))
                .matches("VALUES (1, BIGINT '3'), (2, BIGINT '2'), (3, BIGINT '1'), (5, BIGINT '0')");
    }

    @Test
    public void testScalarAggregateWithNonTrivialJoinFilter()
    {
        // INNER JOIN LATERAL over a scalar aggregate with an ON condition — the correlated-join
        // filter (`l.s > 1`) is applied on top of the per-outer-row aggregate. Legacy can't.
        //   a=1: count(b<1)=0; a=2: count(b<2)=1; a=3: count(b<3)=2; a=4: count(b<4)=3
        //   filter s>1 keeps a=3 (2) and a=4 (3)
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.s FROM (VALUES 1, 2, 3, 4) t(a) " +
                        "JOIN LATERAL (SELECT count(*) AS s FROM (VALUES 1, 2, 3, 4) u(b) WHERE u.b < t.a) l(s) ON l.s > 1"))
                .matches("VALUES (3, BIGINT '2'), (4, BIGINT '3')");
    }

    @Test
    public void testScalarSumWithCorrelatedFilterMaskEmptyIsNull()
    {
        // sum(u.b) FILTER (WHERE u.b > t.a): empty filter result → NULL (not 0), confirming the
        // combined mask folds the empty group to the aggregate-over-empty value.
        //   a=1: sum{2,3,4}=9; a=3: sum{4}=4; a=5: {} → NULL
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, (SELECT sum(u.b) FILTER (WHERE u.b > t.a) FROM (VALUES 1, 2, 3, 4) u(b)) " +
                        "FROM (VALUES 1, 3, 5) t(a)"))
                .matches("VALUES (1, BIGINT '9'), (3, BIGINT '4'), (5, CAST(NULL AS BIGINT))");
    }

    @Test
    public void testCorrelatedUnnestInsideLateralWithFilter()
    {
        // UNNEST nested inside a LATERAL subquery, under an aggregate with a correlated filter
        // (`WHERE x > a`). Unlike a bare CROSS JOIN UNNEST (planned natively as an UnnestNode with
        // no correlated join), this arrives as a CorrelatedJoinNode and is decorrelated through the
        // framework's visitUnnest. DecorrelateUnnest rejects the correlated filter on the
        // unnested value, so this is framework-only.
        //   a=1 [10,20]: x>1 → {10,20} sum 30; a=2 [5]: x>2 → {5} sum 5; a=3 [1]: x>3 → {} sum NULL
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.s FROM (VALUES (1, ARRAY[10, 20]), (2, ARRAY[5]), (3, ARRAY[1])) t(a, arr), " +
                        "LATERAL (SELECT sum(x) AS s FROM UNNEST(arr) u(x) WHERE x > a) l(s)"))
                .matches("VALUES (1, BIGINT '30'), (2, BIGINT '5'), (3, CAST(NULL AS BIGINT))");
    }

    @Test
    public void testCorrelatedLeftUnnestNullExtends()
    {
        // LEFT JOIN UNNEST over an empty array null-extends the outer row — the magic-set path with
        // the CorrelationRebinder rewriting the UnnestNode's array reference onto the input clone.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, u.x FROM (VALUES (1, ARRAY[10, 20]), (2, CAST(ARRAY[] AS ARRAY(integer)))) t(a, arr) " +
                        "LEFT JOIN UNNEST(arr) u(x) ON true"))
                .matches("VALUES (1, 10), (1, 20), (2, CAST(NULL AS INTEGER))");
    }

    @Test
    public void testLeftJoinInsideSubqueryPreservingSideCorrelated()
    {
        // LEFT outer join INSIDE the subquery, with the dependency confined to the preserving
        // (left) side: T₁ pushes into the left, the outer join with the correlation-free right
        // side is preserved. Legacy can't decorrelate an outer join inside the subquery.
        //   t=1: w(u.b<=1)={1}      → 1 has no v.c match → (1, null)
        //   t=2: w={1,2}            → 1→null, 2→2
        //   t=5: w={1,2,3}          → 1→null, 2→2, 3→3
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b, l.c FROM (VALUES 1, 2, 5) t(a), LATERAL (" +
                        "  SELECT w.b, v.c FROM (SELECT u.b FROM (VALUES 1, 2, 3) u(b) WHERE u.b <= t.a) w " +
                        "  LEFT JOIN (VALUES 2, 3) v(c) ON w.b = v.c) l(b, c)"))
                .matches("VALUES (1, 1, CAST(NULL AS INTEGER)), (2, 1, CAST(NULL AS INTEGER)), (2, 2, 2), " +
                        "(5, 1, CAST(NULL AS INTEGER)), (5, 2, 2), (5, 3, 3)");
    }

    @Test
    public void testRightJoinInsideSubqueryPreservingSideCorrelated()
    {
        // RIGHT outer join inside the subquery — symmetric to the LEFT case: the preserving (right)
        // side carries the dependency, so T₁ pushes into the right while the correlation-free left
        // side stays put. Same result shape as the LEFT case.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b, l.c FROM (VALUES 1, 2, 5) t(a), LATERAL (" +
                        "  SELECT w.b, v.c FROM (VALUES 2, 3) v(c) " +
                        "  RIGHT JOIN (SELECT u.b FROM (VALUES 1, 2, 3) u(b) WHERE u.b <= t.a) w ON w.b = v.c) l(b, c)"))
                .matches("VALUES (1, 1, CAST(NULL AS INTEGER)), (2, 1, CAST(NULL AS INTEGER)), (2, 2, 2), " +
                        "(5, 1, CAST(NULL AS INTEGER)), (5, 2, 2), (5, 3, 3)");
    }

    @Test
    public void testCorrelatedGroupingSetsInLateral()
    {
        // GroupId pushdown — GROUPING SETS inside a LATERAL returns multiple rows per outer row, so
        // legacy can't decorrelate it (only the EXISTS form); this exercises visitGroupId directly.
        // T₁'s columns are added to every grouping set, computing the sets per outer row. Non-global
        // sets produce no rows over an empty per-outer-row source, so re-keying them is sound.
        //   t=1: u.k=1 → {(1,10),(1,20)} → set (u.k): (1,NULL) cnt=2; set (u.b): (NULL,10) cnt=1, (NULL,20) cnt=1
        //   t=2: u.k=2 → {(2,30)}        → set (u.k): (2,NULL) cnt=1; set (u.b): (NULL,30) cnt=1
        //   t=5: no match → empty source → no rows (INNER lateral drops t=5)
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.k, l.b, l.cnt FROM (VALUES 1, 2, 5) t(a), LATERAL (" +
                        "  SELECT u.k AS k, u.b AS b, count(*) AS cnt FROM (VALUES (1, 10), (1, 20), (2, 30)) u(k, b) " +
                        "  WHERE u.k = t.a GROUP BY GROUPING SETS ((u.k), (u.b))) l(k, b, cnt)"))
                .matches("VALUES (1, 1, CAST(NULL AS INTEGER), BIGINT '2'), " +
                        "(1, CAST(NULL AS INTEGER), 10, BIGINT '1'), " +
                        "(1, CAST(NULL AS INTEGER), 20, BIGINT '1'), " +
                        "(2, 2, CAST(NULL AS INTEGER), BIGINT '1'), " +
                        "(2, CAST(NULL AS INTEGER), 30, BIGINT '1')");
    }

    @Test
    public void testGroupingSetsWithGlobalSetFailAsUnsupported()
    {
        // A global grouping set — an explicit `()` set, ROLLUP, CUBE — produces a default row
        // (count = 0) over an empty source, so for an outer row with no matches the lateral must
        // yield (NULL, 0), not drop the row. Re-keying the sets on the input columns loses that
        // row, so the framework declines; legacy also rejects the non-EXISTS lateral form.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.k, l.cnt FROM (VALUES 1, 5) t(a), LATERAL (" +
                        "  SELECT u.k AS k, count(*) AS cnt FROM (VALUES (1, 10), (1, 20), (2, 30)) u(k, b) " +
                        "  WHERE u.k = t.a GROUP BY GROUPING SETS ((u.k), ())) l(k, cnt)"))
                .failure().hasMessageContaining("Given correlated subquery is not supported");
    }

    @Test
    public void testGlobalAggregationBelowFilter()
    {
        // A GLOBAL aggregation below a HAVING / filter produces a count = 0 row over an empty
        // per-outer-row source, so re-keying it on the input columns would silently lose exactly
        // that row (the empty-group-loss bug). The single-step pushdown instead exposes the
        // aggregation at the top of a residual dependent join, where the scalar-aggregate form
        // restores the empty group with a mask — these shapes answer correctly where legacy
        // rejects them as unsupported.
        //   EXISTS(… HAVING count(*) = 0) is true exactly when no row matches → only a = 2.
        assertThat(assertions.query(frameworkSession(),
                "SELECT a FROM (VALUES 1, 2) t(a) WHERE EXISTS " +
                        "(SELECT 1 FROM (VALUES 1) u(b) WHERE u.b = t.a HAVING count(*) = 0)"))
                .matches("VALUES 2");
        //   a = 1 matches one row (count 1, filtered out by c = 0); a = 2 matches none → (2, 0).
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.c FROM (VALUES 1, 2) t(a), LATERAL (" +
                        "  SELECT c FROM (SELECT count(*) AS c FROM (VALUES 1) u(b) WHERE u.b = t.a) WHERE c = 0) l(c)"))
                .matches("VALUES (2, BIGINT '0')");
    }

    @Test
    public void testCorrelatedLimitWithTiesInLateral()
    {
        // LIMIT … WITH TIES pushdown — rank() (not row_number) partitioned by the unique input id,
        // so the nth row plus all rows tying with it are kept per outer row. Legacy can't
        // decorrelate WITH TIES in a plain LATERAL.
        //   t=1: u.b>1 → {2,2,3,3,3}, FETCH FIRST 1 WITH TIES → both 2s
        //   t=2: u.b>2 → {3,3,3},     FETCH FIRST 1 WITH TIES → all three 3s
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 1, 2) t(a), LATERAL (" +
                        "  SELECT u.b FROM (VALUES 1, 2, 2, 3, 3, 3) u(b) WHERE u.b > t.a " +
                        "  ORDER BY u.b FETCH FIRST 1 ROWS WITH TIES) l(b)"))
                .matches("VALUES (1, 2), (1, 2), (2, 3), (2, 3), (2, 3)");
    }

    @Test
    public void testCorrelatedInPredicateWithCorrelationInProjection()
    {
        // Correlated IN whose subquery has correlation in a PROJECTION (`u.b * t.a`). The legacy IN
        // rule (TransformCorrelatedInPredicateToJoin) only pulls correlation out of filters, so it
        // can't decorrelate this; the framework lowers IN to a CorrelatedJoin over a global count
        // aggregation and decorrelates the correlated projection.
        //   t=(1,10): {10,20,30} ∋ 10 → kept; t=(2,20): {20,40,60} ∋ 20 → kept;
        //   t=(3,99): {30,60,90} ∌ 99 → dropped
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, t.c FROM (VALUES (1, 10), (2, 20), (3, 99)) t(a, c) " +
                        "WHERE t.c IN (SELECT u.b * t.a FROM (VALUES 10, 20, 30) u(b))"))
                .matches("VALUES (1, 10), (2, 20)");
    }

    @Test
    public void testCorrelatedWindowRowNumber()
    {
        // Window function inside a LATERAL: row_number() OVER (ORDER BY u.b). The framework adds a
        // unique id of the input to the PARTITION BY so the window restarts per outer row. Legacy
        // can't decorrelate a correlated window. a=6 getting rn=1 (not 4) proves per-outer-row.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b, l.rn FROM (VALUES 1, 6) t(a), LATERAL (" +
                        "  SELECT u.b, row_number() OVER (ORDER BY u.b) AS rn FROM (VALUES 5, 6, 7) u(b) WHERE u.b > t.a) l(b, rn)"))
                .matches("VALUES (1, 5, BIGINT '1'), (1, 6, BIGINT '2'), (1, 7, BIGINT '3'), (6, 7, BIGINT '1')");
    }

    @Test
    public void testCorrelatedWindowAggregate()
    {
        // An aggregate window (sum OVER ()) inside a LATERAL — evaluated per outer row via the
        // injected unique-id partition. a=6 summing to 7 (not 18) confirms per-outer-row partitioning.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b, l.w FROM (VALUES 1, 6) t(a), LATERAL (" +
                        "  SELECT u.b, sum(u.b) OVER () AS w FROM (VALUES 5, 6, 7) u(b) WHERE u.b > t.a) l(b, w)"))
                .matches("VALUES (1, 5, BIGINT '18'), (1, 6, BIGINT '18'), (1, 7, BIGINT '18'), (6, 7, BIGINT '7')");
    }

    @Test
    public void testLeftJoinInsideSubqueryNullSupplyingSideCorrelated()
    {
        // LEFT outer join inside the subquery where the NULL-SUPPLYING (right) side carries the
        // correlation and the preserving (left) side is correlation-free. Handled by cross-joining
        // the preserving side into the input and decorrelating via the LEFT magic-set. Legacy can't.
        //   w(t) = {v.c <= t.a}; u LEFT JOIN w ON u.b = w.c:
        //   t=1: w={}      → all null;  t=2: w={2} → only b=2 matches; t=3: w={2,3} → b=2,b=3 match
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b, l.c FROM (VALUES 1, 2, 3) t(a), LATERAL (" +
                        "  SELECT u.b, w.c FROM (VALUES 1, 2, 3) u(b) " +
                        "  LEFT JOIN (SELECT v.c FROM (VALUES 2, 3) v(c) WHERE v.c <= t.a) w ON u.b = w.c) l(b, c)"))
                .matches("VALUES (1, 1, CAST(NULL AS INTEGER)), (1, 2, CAST(NULL AS INTEGER)), (1, 3, CAST(NULL AS INTEGER)), " +
                        "(2, 1, CAST(NULL AS INTEGER)), (2, 2, 2), (2, 3, CAST(NULL AS INTEGER)), " +
                        "(3, 1, CAST(NULL AS INTEGER)), (3, 2, 2), (3, 3, 3)");
    }

    @Test
    public void testRightJoinInsideSubqueryNullSupplyingSideCorrelated()
    {
        // Symmetric: RIGHT outer join with the null-supplying (left) side correlated. `w RIGHT JOIN
        // u` preserves u, so the result matches the LEFT case above.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b, l.c FROM (VALUES 1, 2, 3) t(a), LATERAL (" +
                        "  SELECT u.b, w.c FROM (SELECT v.c FROM (VALUES 2, 3) v(c) WHERE v.c <= t.a) w " +
                        "  RIGHT JOIN (VALUES 1, 2, 3) u(b) ON u.b = w.c) l(b, c)"))
                .matches("VALUES (1, 1, CAST(NULL AS INTEGER)), (1, 2, CAST(NULL AS INTEGER)), (1, 3, CAST(NULL AS INTEGER)), " +
                        "(2, 1, CAST(NULL AS INTEGER)), (2, 2, 2), (2, 3, CAST(NULL AS INTEGER)), " +
                        "(3, 1, CAST(NULL AS INTEGER)), (3, 2, 2), (3, 3, 3)");
    }

    @Test
    public void testCorrelatedTopNInLateral()
    {
        // TopN pushdown — ORDER BY … LIMIT inside a LATERAL. For each outer row, take the
        // smallest matching value. Deterministic because of the ORDER BY.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 1, 2) t(a), " +
                        "  LATERAL (SELECT u.b FROM (VALUES 30, 10, 20) u(b) WHERE u.b > t.a * 10 ORDER BY u.b LIMIT 1) l(b)"))
                .matches("VALUES (1, 20), (2, 30)");
    }

    @Test
    public void testCorrelatedUnnestTopNDefersToDecorrelateUnnest()
    {
        // A bound over an UNNEST of the correlation defers to DecorrelateUnnest (join-free,
        // element-order-preserving); the magic-set would clone and join back on the array value.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.x FROM (VALUES (1, ARRAY[30, 10, 20]), (2, ARRAY[5])) t(a, arr), " +
                        "LATERAL (SELECT u.x FROM UNNEST(arr) u(x) ORDER BY u.x LIMIT 2) l(x)"))
                .matches("VALUES (1, 10), (1, 20), (2, 5)");
    }

    @Test
    public void testCorrelatedUnnestScalarDefersToDecorrelateUnnest()
    {
        // EnforceSingleRow over an UNNEST of the correlation — also DecorrelateUnnest's class;
        // empty arrays NULL-fill.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, (SELECT u.x FROM UNNEST(arr) u(x)) " +
                        "FROM (VALUES (1, ARRAY[7]), (2, CAST(ARRAY[] AS array(integer)))) t(a, arr)"))
                .matches("VALUES (1, 7), (2, CAST(NULL AS INTEGER))");
    }

    @Test
    public void testLeftLateralTopNEqualityCorrelation()
    {
        // LEFT JOIN LATERAL … ORDER BY … LIMIT with equality correlation: the bound is re-emitted
        // below a single plain LEFT join (TopNRankingNode partitioned by the correlated key) —
        // no magic-set clone.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 1, 2, 3) t(a) LEFT JOIN LATERAL (" +
                        "  SELECT u.b FROM (VALUES (1, 10), (1, 20), (2, 30)) u(k, b) WHERE u.k = t.a ORDER BY u.b LIMIT 1) l(b) ON true"))
                .matches("VALUES (1, 10), (2, 30), (3, CAST(NULL AS INTEGER))");
    }

    @Test
    public void testLeftLateralBareLimitOneEqualityCorrelation()
    {
        // Bare LIMIT 1 (no ORDER BY) over a non-constant output — legacy's Limit(1) decorrelation
        // requires all-constant outputs and rejects this; the framework's row-cap form covers it.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 1, 2) t(a) LEFT JOIN LATERAL (" +
                        "  SELECT u.b FROM (VALUES (1, 10), (2, 30)) u(k, b) WHERE u.k = t.a LIMIT 1) l(b) ON true"))
                .matches("VALUES (1, 10), (2, 30)");
    }

    @Test
    public void testLeftLateralAsOfTopN()
    {
        // LEFT-flavored ASOF join — mixed equality + inequality correlation under FETCH FIRST 1.
        // The inequality column is not group-constant, so the bound cannot move below a plain
        // join; the magic-set (per-distinct-correlation evaluation + LEFT join-back) handles it.
        // Legacy rejects this shape entirely.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.id, q.price FROM (VALUES (1, 'A', 10), (2, 'A', 20), (3, 'C', 15)) t(id, symbol, ts) LEFT JOIN LATERAL (" +
                        "  SELECT q.price FROM (VALUES ('A', 5, 100), ('A', 15, 110), ('B', 10, 200)) q(symbol, ts, price) " +
                        "  WHERE q.symbol = t.symbol AND q.ts <= t.ts " +
                        "  ORDER BY q.ts DESC FETCH FIRST 1 ROW ONLY) q ON true"))
                .matches("VALUES (1, 100), (2, 110), (3, CAST(NULL AS INTEGER))");
    }

    @Test
    public void testLeftLateralDistinct()
    {
        // LEFT JOIN LATERAL (SELECT DISTINCT …) — one LEFT join of the uniquely-tagged input with
        // the dedup re-applied above it (legacy TransformCorrelatedDistinctAggregation* parity, no
        // magic-set clone); duplicate outer rows each keep their own distinct set.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 1, 1, 2, 3) t(a) LEFT JOIN LATERAL (" +
                        "  SELECT DISTINCT u.b FROM (VALUES (1, 10), (1, 10), (1, 20), (2, 30)) u(k, b) WHERE u.k = t.a) l(b) ON true"))
                .matches("VALUES (1, 10), (1, 20), (1, 10), (1, 20), (2, 30), (3, CAST(NULL AS INTEGER))");
    }

    @Test
    public void testFilterOnlyScalarSubqueryAtMostOneRowBody()
    {
        // The body is provably at most one row (a filter over a single-row VALUES), so the
        // cardinality-aware path emits a plain NULL-extending LEFT join with no runtime
        // row-count check — mirroring legacy TransformCorrelatedScalarSubquery.
        assertThat(assertions.query(
                frameworkSession(),
                "SELECT a, (SELECT u.b FROM (VALUES (1, 10)) u(k, b) WHERE u.k = t.a) FROM (VALUES 1, 2) t(a)"))
                .matches("VALUES (1, 10), (2, CAST(NULL AS INTEGER))");
    }

    @Test
    public void testFilterOnlyScalarSubqueryMultipleRowsError()
    {
        // A general body gets one LEFT join of the uniquely-tagged input plus a MarkDistinct
        // row-count check, which must raise the standard error when an outer row matches twice.
        assertThat(assertions.query(
                frameworkSession(),
                "SELECT a, (SELECT u.b FROM (VALUES (1, 10), (1, 20)) u(k, b) WHERE u.k = t.a) FROM (VALUES 1) t(a)"))
                .failure().hasMessageContaining("Scalar sub-query has returned multiple rows");
    }

    @Test
    public void testAsOfJoinLateralTopN()
    {
        // The ASOF-join idiom: the latest quote at or before each trade's timestamp — an equality
        // (symbol) plus inequality (ts) correlated filter under ORDER BY … FETCH FIRST 1. The
        // framework's TopN pushdown applies the correlated filter below the per-outer-row
        // row_number window, so the top row is selected among the rows that match. Legacy rejects
        // this shape (the inequality predicate cannot soundly rise above the TopN).
        String asOfJoin = "SELECT t.id, q.price FROM (VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 15)) t(id, symbol, ts), " +
                "LATERAL (SELECT q.price FROM (VALUES ('A', 5, 100), ('A', 15, 110), ('B', 10, 200), ('B', 20, 210)) q(symbol, ts, price) " +
                "         WHERE q.symbol = t.symbol AND q.ts <= t.ts " +
                "         ORDER BY q.ts DESC FETCH FIRST 1 ROW ONLY) q";
        assertThat(assertions.query(frameworkSession(), asOfJoin))
                .matches("VALUES (1, 100), (2, 110), (3, 200)");
        assertThat(assertions.query(
                assertions.sessionBuilder()
                        .setSystemProperty(USE_LEGACY_DECORRELATOR, "true")
                        .build(),
                asOfJoin))
                .failure().hasMessageContaining("Given correlated subquery is not supported");
    }

    @Test
    public void testNonDeterministicInputDeclines()
    {
        // The magic-set clones the input and joins the clone back on value equality, so a
        // non-deterministic input (the clone re-evaluates uuid() to different values) must be
        // declined — committing would silently match nothing. Legacy answers this query (it
        // re-keys via AssignUniqueId instead of cloning); a uid-based fallback is a possible
        // follow-up. Regression test for the missing copyInput determinism guard.
        String query = "SELECT count(s.m) FROM (SELECT CAST(uuid() AS varchar) u FROM (VALUES 1, 2, 3)) o " +
                "LEFT JOIN LATERAL (SELECT max(x) m FROM (VALUES 'a', 'b') v(x) WHERE length(x) <= length(o.u) GROUP BY x) s ON true";
        assertThat(assertions.query(frameworkSession(), query))
                .failure().hasMessageContaining("Given correlated subquery is not supported");
        assertThat(assertions.query(legacySession(), query))
                .matches("VALUES BIGINT '6'");
    }

    @Test
    public void testCorrelatedValuesInLateral()
    {
        // VALUES rows can carry correlation, which the rebinder does not cover — the magic-set
        // must decline (verified post-rebind) instead of committing a dangling reference.
        // Regression tests for the unverified-rebind bug (both used to raise internal errors).
        //   Single-row VALUES: the surviving correlated join is handled by
        //   TransformCorrelatedSingleRowSubqueryToProject in a later pass, like on master.
        assertThat(assertions.query(
                frameworkSession(),
                "SELECT a, b FROM (VALUES 1, 2) t(a) LEFT JOIN LATERAL (VALUES a) t2(b) ON true"))
                .matches("VALUES (1, 1), (2, 2)");
        //   Multi-row VALUES: nothing accepts it — the clean unsupported error, as on master.
        assertThat(assertions.query(
                frameworkSession(),
                "SELECT a, b FROM (VALUES 1, 2) t(a) LEFT JOIN LATERAL (VALUES a, a + 10) t2(b) ON true"))
                .failure().hasMessageContaining("Given correlated subquery is not supported");
    }

    @Test
    public void testNonStrictLiftedProjectionNullExtends()
    {
        // A projection lifted above the plain LEFT join must EVALUATE to NULL for unmatched rows,
        // not merely reference a null-extended symbol: coalesce and IS NULL pass a reference check
        // but stay non-null on the null-extended row. They must route through the magic-set, which
        // keeps the projection below the null-extending join-back. Regression tests for the
        // reference-vs-null-on-null bug (both used to return the non-null value for a = 2).
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.v FROM (VALUES 1, 2) t(a) LEFT JOIN LATERAL (" +
                        "  SELECT coalesce(u.x, t.a) AS v FROM (VALUES 1) u(x) WHERE u.x = t.a) l ON true"))
                .matches("VALUES (1, 1), (2, CAST(NULL AS INTEGER))");
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.v FROM (VALUES 1, 2) t(a) LEFT JOIN LATERAL (" +
                        "  SELECT u.x IS NULL AS v FROM (VALUES 1) u(x) WHERE u.x = t.a) l ON true"))
                .matches("VALUES (1, false), (2, CAST(NULL AS BOOLEAN))");
    }

    @Test
    public void testNonDeterministicEqualityDoesNotPin()
    {
        // An equality whose other side is non-deterministic (`u.c = t.a + random() * 0`) pins
        // nothing — the value is re-sampled per joined row — so the bound-below-join and semi-join
        // forms must not treat the column as group-constant. Results stay correct via the
        // uid-partitioned forms. Regression tests for the dropped isDeterministic clause.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.b FROM (VALUES 1) t(a), LATERAL (" +
                        "  SELECT u.b FROM (VALUES (1, 10), (1, 20)) u(c, b) WHERE u.c = t.a + random() * 0 ORDER BY u.b LIMIT 1) l(b)"))
                .matches("VALUES (1, 10)");
        assertThat(assertions.query(frameworkSession(),
                "SELECT count(*) FROM (VALUES 1, 1) t(a) WHERE EXISTS " +
                        "(SELECT 1 FROM (VALUES 1, 1, 1) u(c) WHERE u.c = t.a + random() * 0)"))
                .matches("VALUES BIGINT '2'");
    }

    @Test
    public void testExistenceBoundBelowCollectedFilter()
    {
        // Dropping a row bound is sound only relative to what sits BELOW it: for the filter
        // x = o.k collected above the TopN, "some row of the top-2 passes" differs from "some row
        // passes". k = 1 matches a t row, but not within the top-2 by y — the existence aggregate
        // over the empty filtered set must be NULL, not TRUE. Regression test for the
        // bound-drop-under-collected-filters bug.
        assertThat(assertions.query(frameworkSession(),
                "SELECT o.k, (SELECT bool_or(true) FROM (" +
                        "  SELECT * FROM (VALUES (1, 10), (2, 1), (2, 2)) t(x, y) ORDER BY y LIMIT 2) v " +
                        "  WHERE v.x = o.k) FROM (VALUES 1, 2) o(k)"))
                .matches("VALUES (1, CAST(NULL AS BOOLEAN)), (2, true)");
    }

    @Test
    public void testNullCorrelationThroughMagicSet()
    {
        // A NULL correlation key through the actual magic-set (correlation in the projection, so
        // the plain-LEFT-join form declines): the IS NOT DISTINCT FROM join-back matches the NULL
        // group, and NULL + 10 null-extends to NULL.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.x FROM (VALUES 1, CAST(NULL AS integer)) t(a) LEFT JOIN LATERAL (" +
                        "  SELECT t.a + u.b AS x FROM (VALUES 10) u(b)) l(x) ON true"))
                .matches("VALUES (1, 11), (CAST(NULL AS integer), CAST(NULL AS integer))");
    }

    @Test
    public void testNaNCorrelationThroughMagicSet()
    {
        // NaN correlation keys through the magic-set: grouping and the IDENTICAL join-back use
        // the same equivalence (all NaNs alike), consistent with tuple-at-a-time evaluation.
        assertThat(assertions.query(frameworkSession(),
                "SELECT t.a, l.x FROM (VALUES nan(), 1e0) t(a) LEFT JOIN LATERAL (" +
                        "  SELECT t.a + u.b AS x FROM (VALUES 1e0) u(b)) l(x) ON true"))
                .matches("VALUES (nan(), nan()), (1e0, 2e0)");
    }

    @Test
    public void testScalarProjectionCorrelatedMultipleRowsFails()
    {
        // The multiple-rows runtime check must also fire through the projection-correlated scalar
        // path (magic-set + per-group row_number), not just the filter-only forms.
        assertThat(assertions.query(
                frameworkSession(),
                "SELECT (SELECT t.a + u.b FROM (VALUES 10, 20) u(b)) FROM (VALUES 1) t(a)"))
                .failure().hasMessageContaining("Scalar sub-query has returned multiple rows");
    }
}
