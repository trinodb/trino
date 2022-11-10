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

package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.Test;

import static io.trino.SystemSessionProperties.SCALE_WRITERS;
import static io.trino.SystemSessionProperties.TASK_SCALE_WRITERS_ENABLED;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableWriter;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestAddLocalExchangesForTaskScaleWriters
        extends BasePlanTest
{
    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();
        LocalQueryRunner queryRunner = LocalQueryRunner.create(session);
        queryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        queryRunner.createCatalog("mock_dont_report_written_bytes", createConnectorFactorySupportingReportingBytesWritten(false, "mock_dont_report_written_bytes"), ImmutableMap.of());
        queryRunner.createCatalog("mock_report_written_bytes", createConnectorFactorySupportingReportingBytesWritten(true, "mock_report_written_bytes"), ImmutableMap.of());
        return queryRunner;
    }

    private MockConnectorFactory createConnectorFactorySupportingReportingBytesWritten(boolean supportsWrittenBytes, String name)
    {
        return MockConnectorFactory.builder()
                .withSupportsReportingWrittenBytes(supportsWrittenBytes)
                .withGetTableHandle(((session, schemaTableName) -> null))
                .withName(name)
                .build();
    }

    @Test
    public void testLocalScaledWriterDistributionWithSupportsReportingWrittenBytes()
    {
        assertDistributedPlan(
                "CREATE TABLE mock_report_written_bytes.mock.test AS SELECT nationkey FROM tpch.tiny.nation",
                testSessionBuilder()
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "true")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("nationkey"),
                                ImmutableList.of("nationkey"),
                                exchange(LOCAL, REPARTITION, SCALED_WRITER_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))))));

        assertDistributedPlan(
                "CREATE TABLE mock_report_written_bytes.mock.test AS SELECT nationkey FROM tpch.tiny.nation",
                testSessionBuilder()
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "false")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("nationkey"),
                                ImmutableList.of("nationkey"),
                                exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))))));
    }

    @Test
    public void testLocalScaledWriterDistributionWithoutSupportsReportingWrittenBytes()
    {
        assertDistributedPlan(
                "CREATE TABLE mock_dont_report_written_bytes.mock.test AS SELECT nationkey FROM tpch.tiny.nation",
                testSessionBuilder()
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "true")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("nationkey"),
                                ImmutableList.of("nationkey"),
                                exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))))));

        assertDistributedPlan(
                "CREATE TABLE mock_dont_report_written_bytes.mock.test AS SELECT nationkey FROM tpch.tiny.nation",
                testSessionBuilder()
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "false")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("nationkey"),
                                ImmutableList.of("nationkey"),
                                exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))))));
    }
}
