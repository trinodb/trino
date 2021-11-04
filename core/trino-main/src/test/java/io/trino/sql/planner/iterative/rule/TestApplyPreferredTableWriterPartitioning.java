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
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.RuleAssert;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.SystemSessionProperties.PREFERRED_WRITE_PARTITIONING_MIN_NUMBER_OF_PARTITIONS;
import static io.trino.SystemSessionProperties.USE_PREFERRED_WRITE_PARTITIONING;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableWriter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.RuleTester.defaultRuleTester;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.Double.NaN;

public class TestApplyPreferredTableWriterPartitioning
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String NODE_ID = "mock";
    private static final double NO_STATS = -1;

    private static final Session SESSION_WITHOUT_PREFERRED_PARTITIONING = testSessionBuilder()
            .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "false")
            .setCatalog(MOCK_CATALOG)
            .setSchema(TEST_SCHEMA)
            .build();
    private static final Session SESSION_WITH_PREFERRED_PARTITIONING_THRESHOLD_0 = testSessionBuilder()
            .setCatalog(MOCK_CATALOG)
            .setSchema(TEST_SCHEMA)
            .setSystemProperty(PREFERRED_WRITE_PARTITIONING_MIN_NUMBER_OF_PARTITIONS, "1")
            .build();
    private static final Session SESSION_WITH_PREFERRED_PARTITIONING_DEFAULT_THRESHOLD = testSessionBuilder()
            .setCatalog(MOCK_CATALOG)
            .setSchema(TEST_SCHEMA)
            .build();

    private static final PlanMatchPattern SUCCESSFUL_MATCH = tableWriter(
            ImmutableList.of(),
            ImmutableList.of(),
            values(0));

    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = defaultRuleTester();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }

    @Test(dataProvider = "preferWritePartitioningDataProvider")
    public void testPreferWritePartitioning(Session session, double distinctValuesStat, boolean match)
    {
        RuleAssert ruleAssert = assertPreferredPartitioning(
                new PartitioningScheme(
                        Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of(new Symbol("col_one"))),
                        ImmutableList.of(new Symbol("col_one"))))
                .withSession(session);
        if (distinctValuesStat != NO_STATS) {
            ruleAssert = ruleAssert.overrideStats(NODE_ID, PlanNodeStatsEstimate.builder()
                    .addSymbolStatistics(ImmutableMap.of(
                            new Symbol("col_one"),
                            new SymbolStatsEstimate(0, 0, 0, 0, distinctValuesStat)))
                    .build());
        }
        if (match) {
            ruleAssert.matches(SUCCESSFUL_MATCH);
        }
        else {
            ruleAssert.doesNotFire();
        }
    }

    @DataProvider(name = "preferWritePartitioningDataProvider")
    public Object[][] preferWritePartitioningDataProvider()
    {
        return new Object[][] {
                new Object[] {SESSION_WITHOUT_PREFERRED_PARTITIONING, NO_STATS, false},
                new Object[] {SESSION_WITHOUT_PREFERRED_PARTITIONING, NaN, false},
                new Object[] {SESSION_WITHOUT_PREFERRED_PARTITIONING, 1, false},
                new Object[] {SESSION_WITHOUT_PREFERRED_PARTITIONING, 50, false},
                new Object[] {SESSION_WITH_PREFERRED_PARTITIONING_THRESHOLD_0, NO_STATS, true},
                new Object[] {SESSION_WITH_PREFERRED_PARTITIONING_THRESHOLD_0, NaN, true},
                new Object[] {SESSION_WITH_PREFERRED_PARTITIONING_THRESHOLD_0, 1, true},
                new Object[] {SESSION_WITH_PREFERRED_PARTITIONING_THRESHOLD_0, 49, true},
                new Object[] {SESSION_WITH_PREFERRED_PARTITIONING_THRESHOLD_0, 50, true},
                new Object[] {SESSION_WITH_PREFERRED_PARTITIONING_DEFAULT_THRESHOLD, NO_STATS, false},
                new Object[] {SESSION_WITH_PREFERRED_PARTITIONING_DEFAULT_THRESHOLD, NaN, false},
                new Object[] {SESSION_WITH_PREFERRED_PARTITIONING_DEFAULT_THRESHOLD, 1, false},
                new Object[] {SESSION_WITH_PREFERRED_PARTITIONING_DEFAULT_THRESHOLD, 49, false},
                new Object[] {SESSION_WITH_PREFERRED_PARTITIONING_DEFAULT_THRESHOLD, 50, true},
        };
    }

    @Test
    public void testThresholdWithNullFraction()
    {
        // Null value in partition column should increase the number of partitions by 1
        PlanNodeStatsEstimate stats = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(ImmutableMap.of(new Symbol("col_one"), new SymbolStatsEstimate(0, 0, .5, 0, 49)))
                .build();

        assertPreferredPartitioning(new PartitioningScheme(
                Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of(new Symbol("col_one"))),
                ImmutableList.of(new Symbol("col_one"))))
                .withSession(SESSION_WITH_PREFERRED_PARTITIONING_DEFAULT_THRESHOLD)
                .overrideStats(NODE_ID, stats)
                .matches(SUCCESSFUL_MATCH);
    }

    @Test
    public void testThresholdWithMultiplePartitions()
    {
        PlanNodeStatsEstimate stats = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(ImmutableMap.of(new Symbol("col_one"), new SymbolStatsEstimate(0, 0, 0, 0, 5)))
                .addSymbolStatistics(ImmutableMap.of(new Symbol("col_two"), new SymbolStatsEstimate(0, 0, 0, 0, 10)))
                .build();

        assertPreferredPartitioning(new PartitioningScheme(
                Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of(new Symbol("col_one"), new Symbol("col_two"))),
                ImmutableList.of(new Symbol("col_one"), new Symbol("col_two"))))
                .withSession(SESSION_WITH_PREFERRED_PARTITIONING_DEFAULT_THRESHOLD)
                .overrideStats(NODE_ID, stats)
                .matches(SUCCESSFUL_MATCH);
    }

    private RuleAssert assertPreferredPartitioning(PartitioningScheme preferredPartitioningScheme)
    {
        return tester.assertThat(new ApplyPreferredTableWriterPartitioning())
                .on(builder -> builder.tableWriter(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.of(preferredPartitioningScheme),
                        Optional.empty(),
                        Optional.empty(),
                        new ValuesNode(new PlanNodeId(NODE_ID), 0)));
    }
}
