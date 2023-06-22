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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushdownLimitIntoRowNumber
        extends BaseRuleTest
{
    @Test
    public void testLimitAboveRowNumber()
    {
        tester().assertThat(new PushdownLimitIntoRowNumber())
                .on(p ->
                        p.limit(
                                3,
                                p.rowNumber(
                                        ImmutableList.of(),
                                        Optional.of(5),
                                        p.symbol("row_number"),
                                        p.values(p.symbol("a")))))
                .matches(
                        rowNumber(rowNumber -> rowNumber
                                        .partitionBy(ImmutableList.of())
                                        .maxRowCountPerPartition(Optional.of(3)),
                                values("a")));

        tester().assertThat(new PushdownLimitIntoRowNumber())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.limit(
                            3,
                            p.rowNumber(
                                    ImmutableList.of(a),
                                    Optional.of(5),
                                    p.symbol("row_number"), p.values(a)));
                })
                .matches(
                        limit(
                                3,
                                rowNumber(rowNumber -> rowNumber
                                                .partitionBy(ImmutableList.of("a"))
                                                .maxRowCountPerPartition(Optional.of(3)),
                                        values("a"))));

        tester().assertThat(new PushdownLimitIntoRowNumber())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.limit(
                            3,
                            p.rowNumber(
                                    ImmutableList.of(a),
                                    Optional.empty(),
                                    p.symbol("row_number"), p.values(a)));
                })
                .matches(
                        limit(
                                3,
                                rowNumber(rowNumber -> rowNumber
                                                .partitionBy(ImmutableList.of("a"))
                                                .maxRowCountPerPartition(Optional.of(3)),
                                        values("a"))));

        tester().assertThat(new PushdownLimitIntoRowNumber())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.limit(
                            5,
                            p.rowNumber(
                                    ImmutableList.of(),
                                    Optional.of(3),
                                    p.symbol("row_number"), p.values(a)));
                })
                .matches(
                        rowNumber(rowNumber -> rowNumber
                                        .partitionBy(ImmutableList.of())
                                        .maxRowCountPerPartition(Optional.of(3)),
                                values("a")));
    }

    @Test
    public void testZeroLimit()
    {
        tester().assertThat(new PushdownLimitIntoRowNumber())
                .on(p ->
                        p.limit(
                                0,
                                p.rowNumber(ImmutableList.of(), Optional.of(5),
                                        p.symbol("row_number"), p.values(p.symbol("a")))))
                .doesNotFire();
    }

    @Test
    public void testTiesLimit()
    {
        tester().assertThat(new PushdownLimitIntoRowNumber())
                .on(p ->
                        p.limit(
                                0,
                                ImmutableList.of(p.symbol("a")),
                                p.rowNumber(
                                        ImmutableList.of(),
                                        Optional.of(5),
                                        p.symbol("row_number"),
                                        p.values(p.symbol("a")))))
                .doesNotFire();
    }

    @Test
    public void testIdenticalLimit()
    {
        tester().assertThat(new PushdownLimitIntoRowNumber())
                .on(p ->
                        p.limit(
                                5,
                                ImmutableList.of(p.symbol("a")),
                                p.rowNumber(
                                        ImmutableList.of(),
                                        Optional.of(5),
                                        p.symbol("row_number"),
                                        p.values(p.symbol("a")))))
                .doesNotFire();
    }

    @Test
    public void testLimitWithPreSortedInputs()
    {
        tester().assertThat(new PushdownLimitIntoRowNumber())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.limit(
                            5,
                            false,
                            ImmutableList.of(a),
                            p.rowNumber(
                                    ImmutableList.of(),
                                    Optional.of(3),
                                    p.symbol("row_number"), p.values(a)));
                })
                .matches(
                        rowNumber(rowNumber -> rowNumber
                                        .partitionBy(ImmutableList.of())
                                        .maxRowCountPerPartition(Optional.of(3))
                                        .orderSensitive(true),
                                values("a")));
    }
}
