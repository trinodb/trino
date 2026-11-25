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
package io.trino.execution;

import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;

import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.EXECUTION_POLICY;
import static io.trino.SystemSessionProperties.FORCE_SPILLING_JOIN;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.MAX_HASH_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.MIN_HASH_PARTITION_COUNT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Runs joins whose tasks stop reading an exchange before all tasks of the stage feeding it exist.
 * The {@code slow} catalog of {@link SplitsOverridingTpchPlugin} makes its source stage create tasks one after
 * another, and small output buffers make a producer block once a consumer stops reading.
 */
@Execution(SAME_THREAD)
final class TestExchangeConsumersFinishingEarly
        extends AbstractTestQueryFramework
{
    private static final String SINGLE_PART = "SELECT partkey FROM tpch.tiny.part WHERE name = 'goldenrod lavender spring chocolate lace'";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                .setSystemProperty(MIN_HASH_PARTITION_COUNT, "3")
                .setSystemProperty(MAX_HASH_PARTITION_COUNT, "3")
                .build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setWorkerCount(2)
                .addExtraProperty("sink.max-buffer-size", "4kB")
                .addExtraProperty("sink.max-broadcast-buffer-size", "4kB")
                .addExtraProperty("query.min-schedule-split-batch-size", "1")
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", Map.of("tpch.splits-per-node", "2"));
            queryRunner.createCatalog("tpch2", "tpch", Map.of("tpch.splits-per-node", "2"));
            SplitsOverridingTpchPlugin.createCatalogs(queryRunner);
        }
        catch (Throwable e) {
            queryRunner.close();
            throw e;
        }
        return queryRunner;
    }

    @Test
    @Timeout(120)
    void testEmptyBuildOverPartitionedExchange()
    {
        assertQueryCompletes(
                getSession(),
                "SELECT count(*) FROM slow.sf1.partsupp ps JOIN (" + SINGLE_PART + ") p ON ps.partkey = p.partkey");
    }

    @Test
    @Timeout(120)
    void testEmptyBuildOverPartitionedExchangeWithSpillingJoin()
    {
        assertQueryCompletes(
                Session.builder(getSession())
                        .setSystemProperty(FORCE_SPILLING_JOIN, "true")
                        .build(),
                "SELECT count(*) FROM slow.sf1.partsupp ps JOIN (" + SINGLE_PART + ") p ON ps.partkey = p.partkey");
    }

    @Test
    @Timeout(120)
    void testEmptyBuildWhileTaskKeepsRunning()
    {
        assertQueryCompletes(
                Session.builder(getSession())
                        .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                        .build(),
                """
                SELECT count(*)
                FROM (SELECT ps.partkey FROM slow.sf1.partsupp ps JOIN (%s) p ON ps.partkey = p.partkey) x
                RIGHT JOIN slow.sf1.partsupp big ON x.partkey = big.partkey
                """.formatted(SINGLE_PART));
    }

    @Test
    @Timeout(120)
    void testEmptyProbeOverPartitionedExchange()
    {
        assertQueryCompletes(
                Session.builder(getSession())
                        .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                        .setSystemProperty(EXECUTION_POLICY, "all-at-once")
                        .build(),
                "SELECT count(*) FROM (" + SINGLE_PART + ") p JOIN slow.sf1.partsupp ps ON ps.partkey = p.partkey");
    }

    @Test
    @Timeout(120)
    void testEmptyProbeOverPartitionedExchangeWithDynamicFiltering()
    {
        assertQueryCompletes(
                Session.builder(getSession())
                        .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                        .setSystemProperty(EXECUTION_POLICY, "all-at-once")
                        .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "true")
                        .build(),
                "SELECT count(*) FROM (" + SINGLE_PART + ") p JOIN slow.sf1.partsupp ps ON ps.partkey = p.partkey");
    }

    @Test
    @Timeout(120)
    void testEmptyProbeOverPartitionedExchangeWithSpillingJoin()
    {
        assertQueryCompletes(
                Session.builder(getSession())
                        .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                        .setSystemProperty(EXECUTION_POLICY, "all-at-once")
                        .setSystemProperty(FORCE_SPILLING_JOIN, "true")
                        .build(),
                "SELECT count(*) FROM (" + SINGLE_PART + ") p JOIN slow.sf1.partsupp ps ON ps.partkey = p.partkey");
    }

    @Test
    @Timeout(120)
    void testEmptyProbeOverBroadcastExchange()
    {
        assertQueryCompletes(
                broadcastAllAtOnceSession(),
                "SELECT count(*) FROM (" + SINGLE_PART + ") p JOIN slow.sf1.partsupp ps ON ps.partkey = p.partkey");
    }

    @Test
    @Timeout(120)
    void testEmptyProbeOverBroadcastExchangeForCrossJoin()
    {
        assertQueryCompletes(
                broadcastAllAtOnceSession(),
                "SELECT count(*) FROM (" + SINGLE_PART + ") p CROSS JOIN slow.sf1.partsupp ps WHERE ps.availqty + p.partkey > 0");
    }

    @Test
    @Timeout(120)
    void testEmptyRoundRobinProbeOverBroadcastExchange()
    {
        assertQueryCompletes(
                broadcastAllAtOnceSession(),
                """
                SELECT count(*)
                FROM (%s UNION ALL SELECT partkey FROM tpch2.tiny.part WHERE name = 'blush thistle blue yellow saddle') p
                JOIN slow.sf1.partsupp ps ON ps.partkey = p.partkey
                """.formatted(SINGLE_PART));
    }

    private Session broadcastAllAtOnceSession()
    {
        return Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .setSystemProperty(EXECUTION_POLICY, "all-at-once")
                .build();
    }

    private void assertQueryCompletes(Session session, String sql)
    {
        Object expected = computeActual(session, sql.replace("slow.", "tpch.")).getOnlyValue();
        assertThat(computeActual(session, sql).getOnlyValue()).isEqualTo(expected);
    }
}
