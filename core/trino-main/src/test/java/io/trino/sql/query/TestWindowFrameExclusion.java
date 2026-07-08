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

import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestWindowFrameExclusion
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testExcludeCurrentRowOverFullFrame()
    {
        // frame is the whole partition; EXCLUDE CURRENT ROW drops just the current row.
        // Values are distinct, so the aggregated array follows ascending order.
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a " +
                "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) " +
                "FROM (VALUES 1, 2, 3, 4) t(a)"))
                .matches("VALUES " +
                        "ARRAY[2, 3, 4], " +
                        "ARRAY[1, 3, 4], " +
                        "ARRAY[1, 2, 4], " +
                        "ARRAY[1, 2, 3]");
    }

    @Test
    public void testExclusionWithPeerGroups()
    {
        // rows with a = 2 form a peer group of two; a = 1 and a = 3 are singleton peer groups
        String data = "FROM (VALUES 1, 2, 2, 3) t(a)";

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a " +
                "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) " + data))
                .matches("VALUES ARRAY[2, 2, 3], ARRAY[1, 2, 3], ARRAY[1, 2, 3], ARRAY[1, 2, 2]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a " +
                "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) " + data))
                .matches("VALUES ARRAY[2, 2, 3], ARRAY[1, 3], ARRAY[1, 3], ARRAY[1, 2, 2]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a " +
                "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) " + data))
                .matches("VALUES ARRAY[1, 2, 2, 3], ARRAY[1, 2, 3], ARRAY[1, 2, 3], ARRAY[1, 2, 2, 3]");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a " +
                "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE NO OTHERS) " + data))
                .matches("VALUES ARRAY[1, 2, 2, 3], ARRAY[1, 2, 2, 3], ARRAY[1, 2, 2, 3], ARRAY[1, 2, 2, 3]");
    }

    @Test
    public void testExclusionWithSlidingRowsFrame()
    {
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a " +
                "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) " +
                "FROM (VALUES 1, 2, 3, 4, 5) t(a)"))
                .matches("VALUES ARRAY[2], ARRAY[1, 3], ARRAY[2, 4], ARRAY[3, 5], ARRAY[4]");
    }

    @Test
    public void testExclusionWithGroupsFrame()
    {
        // GROUPS 1 PRECEDING TO 1 FOLLOWING with peer groups {1}, {2,2}, {3,3,3}
        String data = "FROM (VALUES 1, 2, 2, 3, 3, 3) t(a)";

        // full frame for the a = 2 rows is {1, 2, 2, 3, 3, 3}; EXCLUDE GROUP removes the two 2s
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a " +
                "GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE GROUP) " + data))
                .matches("VALUES " +
                        "ARRAY[2, 2], " +
                        "ARRAY[1, 3, 3, 3], " +
                        "ARRAY[1, 3, 3, 3], " +
                        "ARRAY[2, 2], " +
                        "ARRAY[2, 2], " +
                        "ARRAY[2, 2]");
    }

    @Test
    public void testExclusionOverAggregate()
    {
        String data = "FROM (VALUES 1, 2, 2, 3) t(a)";

        assertThat(assertions.query("SELECT sum(a) OVER(ORDER BY a " +
                "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) " + data))
                .matches("VALUES BIGINT '7', BIGINT '6', BIGINT '6', BIGINT '5'");

        assertThat(assertions.query("SELECT sum(a) OVER(ORDER BY a " +
                "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) " + data))
                .matches("VALUES BIGINT '7', BIGINT '4', BIGINT '4', BIGINT '5'");

        assertThat(assertions.query("SELECT sum(a) OVER(ORDER BY a " +
                "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) " + data))
                .matches("VALUES BIGINT '8', BIGINT '6', BIGINT '6', BIGINT '8'");
    }

    @Test
    public void testExclusionOverValueFunctions()
    {
        String frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW";
        String data = "FROM (VALUES 1, 2, 3, 4, 5) t(a)";

        assertThat(assertions.query("SELECT first_value(a) OVER(ORDER BY a " + frame + ") " + data))
                .matches("VALUES 2, 1, 1, 1, 1");

        assertThat(assertions.query("SELECT last_value(a) OVER(ORDER BY a " + frame + ") " + data))
                .matches("VALUES 5, 5, 5, 5, 4");

        assertThat(assertions.query("SELECT nth_value(a, 2) OVER(ORDER BY a " + frame + ") " + data))
                .matches("VALUES 3, 3, 2, 2, 2");
    }

    @Test
    public void testExclusionWithIgnoreNulls()
    {
        // first_value skips both the excluded row and the nulls
        assertThat(assertions.query("SELECT first_value(a) IGNORE NULLS OVER(ORDER BY b " +
                "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) " +
                "FROM (VALUES (CAST(null AS integer), 1), (2, 2), (3, 3)) t(a, b)"))
                .matches("VALUES 2, 3, 2");
    }

    @Test
    public void testExclusionYieldingEmptyFrame()
    {
        // a single-row frame with EXCLUDE CURRENT ROW leaves the frame empty
        assertThat(assertions.query("SELECT sum(a) OVER(ORDER BY a " +
                "ROWS BETWEEN CURRENT ROW AND CURRENT ROW EXCLUDE CURRENT ROW) " +
                "FROM (VALUES 1, 2, 3) t(a)"))
                .matches("VALUES CAST(null AS bigint), null, null");

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a " +
                "ROWS BETWEEN CURRENT ROW AND CURRENT ROW EXCLUDE CURRENT ROW) " +
                "FROM (VALUES 1, 2, 3) t(a)"))
                .matches("VALUES CAST(null AS array(integer)), null, null");

        // count over an empty frame is 0, not null -- the one aggregate whose empty result is not null
        assertThat(assertions.query("SELECT count(a) OVER(ORDER BY a " +
                "ROWS BETWEEN CURRENT ROW AND CURRENT ROW EXCLUDE CURRENT ROW) " +
                "FROM (VALUES 1, 2, 3) t(a)"))
                .matches("VALUES BIGINT '0', BIGINT '0', BIGINT '0'");

        // value functions take a separate hole-scan path; a fully excluded frame must yield null
        assertThat(assertions.query("SELECT first_value(a) OVER(ORDER BY a " +
                "ROWS BETWEEN CURRENT ROW AND CURRENT ROW EXCLUDE CURRENT ROW) " +
                "FROM (VALUES 1, 2, 3) t(a)"))
                .matches("VALUES CAST(null AS integer), null, null");
    }

    @Test
    public void testExcludeTiesRetainsButNeverAddsCurrentRow()
    {
        // All three rows are peers (equal ordering key). For each row the frame stops at 1 PRECEDING, so it never
        // contains the current row; EXCLUDE TIES must remove the in-frame peers and must NOT re-add the current
        // row (which is outside the frame), leaving every frame empty. An implementation that unconditionally
        // keeps the current row would instead return ARRAY[1] for the second and third rows.
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a " +
                "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING EXCLUDE TIES) " +
                "FROM (VALUES 1, 1, 1) t(a)"))
                .matches("VALUES CAST(null AS array(integer)), null, null");
    }

    @Test
    public void testExclusionWithNullsAndDescendingOrder()
    {
        // null ordering keys are peers of one another. count(*) (not count(a)) is required to discriminate:
        // for the null rows, count(a) would ignore a surviving null and report 1 whether or not the two nulls
        // form one peer group. With count(*), treating the nulls as one group gives 2, 1, 1; treating them as
        // separate groups would give 2, 2, 2.
        assertThat(assertions.query("SELECT count(*) OVER(ORDER BY a " +
                "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) " +
                "FROM (VALUES 1, CAST(null AS integer), CAST(null AS integer)) t(a)"))
                .matches("VALUES BIGINT '2', BIGINT '1', BIGINT '1'");

        // exclusion positions come from the sorted partition, so DESC ordering must be honored
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a DESC " +
                "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) " +
                "FROM (VALUES 1, 2, 3) t(a)"))
                .matches("VALUES ARRAY[2, 1], ARRAY[3, 1], ARRAY[3, 2]");
    }

    @Test
    public void testExclusionWithoutOrderBy()
    {
        // with no ORDER BY the whole partition is a single peer group
        assertThat(assertions.query("SELECT sum(a) OVER(" +
                "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) " +
                "FROM (VALUES 1, 2, 3) t(a)"))
                .matches("VALUES CAST(null AS bigint), null, null");

        assertThat(assertions.query("SELECT sum(a) OVER(" +
                "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) " +
                "FROM (VALUES 1, 2, 3) t(a)"))
                .matches("VALUES BIGINT '1', BIGINT '2', BIGINT '3'");
    }

    @Test
    public void testExclusionAcrossPartitions()
    {
        // A partition whose last row has a hole (EXCLUDE removes a non-contiguous piece) must not leak its
        // aggregation state into the next partition. The frame excludes the current row entirely, so the first
        // row of the second partition has an empty frame with no hole and must be NULL -- exactly the path that
        // leaks a stale accumulator if the per-partition reset does not clear it. In partition p=1 the last row's
        // frame [pos 0, pos 1] overlaps its own peer group (the two k=20 rows), so EXCLUDE GROUP leaves a hole.
        // task_concurrency=1 forces both partitions through a single WindowOperator, which reuses one window
        // function instance across partitions -- the condition under which the leak is observable.
        Session singleDriver = assertions.sessionBuilder()
                .setSystemProperty(TASK_CONCURRENCY, "1")
                .build();
        assertThat(assertions.query(singleDriver, "SELECT sum(v) OVER(PARTITION BY p ORDER BY k " +
                "ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING EXCLUDE GROUP) " +
                "FROM (VALUES (1, 10, 100), (1, 20, 200), (1, 20, 300), (2, 5, 500), (2, 6, 600)) t(p, k, v)"))
                .matches("VALUES CAST(null AS bigint), BIGINT '100', BIGINT '100', null, BIGINT '500'");

        // The second partition contains a peer group with duplicate ordering values whose EXCLUDE GROUP holes
        // are computed from partition-relative coordinates. task_concurrency=1 is required here too: with default
        // concurrency the two partitions hash to separate drivers, each seeing partitionStart == 0, so the
        // partition-relative arithmetic would never be exercised beyond the first partition.
        assertThat(assertions.query(singleDriver, "SELECT array_agg(a) OVER(PARTITION BY p ORDER BY a " +
                "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) " +
                "FROM (VALUES (1, 1), (1, 2), (2, 5), (2, 5), (2, 6)) t(p, a)"))
                .matches("VALUES " +
                        "ARRAY[2], ARRAY[1], " +
                        "ARRAY[6], ARRAY[6], ARRAY[5, 5]");
    }

    @Test
    public void testExclusionRejectedInPatternRecognition()
    {
        assertThat(assertions.query("SELECT last_value(x) OVER(" +
                "ORDER BY x " +
                "MEASURES CLASSIFIER() AS c " +
                "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "EXCLUDE CURRENT ROW " +
                "PATTERN (A+) " +
                "DEFINE A AS true) " +
                "FROM (VALUES 1, 2, 3) t(x)"))
                .failure().hasMessage("line 1:38: Frame exclusion is not allowed with pattern recognition");
    }
}
