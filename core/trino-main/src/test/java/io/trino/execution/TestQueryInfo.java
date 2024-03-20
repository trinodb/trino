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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.stats.Distribution;
import io.airlift.stats.TDigest;
import io.airlift.tracing.SpanSerialization.SpanDeserializer;
import io.airlift.tracing.SpanSerialization.SpanSerializer;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.trino.client.NodeVersion;
import io.trino.operator.RetryPolicy;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.server.BasicQueryStats;
import io.trino.server.ResultQueryInfo;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoWarning;
import io.trino.spi.WarningCode;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.spi.eventlistener.StageGcStatistics;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolKeyDeserializer;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.transaction.TransactionId;
import io.trino.type.TypeSignatureDeserializer;
import io.trino.type.TypeSignatureKeyDeserializer;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.QueryState.FINISHED;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryInfo
{
    private static final TypeManager TYPE_MANAGER = new TestingTypeManager();

    @Test
    public void testQueryInfoRoundTrip()
    {
        JsonCodec<QueryInfo> codec = new JsonCodecFactory(
                new ObjectMapperProvider()
                        .withJsonSerializers(Map.of(
                                Span.class, new SpanSerializer(OpenTelemetry.noop())))
                        .withJsonDeserializers(Map.of(
                                Span.class, new SpanDeserializer(OpenTelemetry.noop()),
                                TypeSignature.class, new TypeSignatureDeserializer()))
                        .withKeyDeserializers(Map.of(
                                TypeSignature.class, new TypeSignatureKeyDeserializer(),
                                Symbol.class, new SymbolKeyDeserializer(TYPE_MANAGER))))
                .jsonCodec(QueryInfo.class);

        QueryInfo expected = createQueryInfo(Optional.empty());
        QueryInfo actual = codec.fromJson(codec.toJsonBytes(expected));

        assertThat(actual.getQueryId()).isEqualTo(expected.getQueryId());
        // Note: SessionRepresentation.equals?
        assertThat(actual.getState()).isEqualTo(expected.getState());
        assertThat(actual.isScheduled()).isEqualTo(expected.isScheduled());
        assertThat(actual.getProgressPercentage()).isEqualTo(expected.getProgressPercentage());
        assertThat(actual.getRunningPercentage()).isEqualTo(expected.getRunningPercentage());

        assertThat(actual.getSelf()).isEqualTo(expected.getSelf());
        assertThat(actual.getFieldNames()).isEqualTo(expected.getFieldNames());
        assertThat(actual.getQuery()).isEqualTo(expected.getQuery());
        assertThat(actual.getPreparedQuery()).isEqualTo(expected.getPreparedQuery());
        // Assert all of queryStats
        TestQueryStats.assertExpectedQueryStats(actual.getQueryStats());

        assertThat(actual.getSetCatalog()).isEqualTo(expected.getSetCatalog());
        assertThat(actual.getSetSchema()).isEqualTo(expected.getSetSchema());
        assertThat(actual.getSetPath()).isEqualTo(expected.getSetPath());
        assertThat(actual.getSetSessionProperties()).isEqualTo(expected.getSetSessionProperties());
        assertThat(actual.getResetSessionProperties()).isEqualTo(expected.getResetSessionProperties());
        assertThat(actual.getSetRoles()).isEqualTo(expected.getSetRoles());
        assertThat(actual.getAddedPreparedStatements()).isEqualTo(expected.getAddedPreparedStatements());
        assertThat(actual.getDeallocatedPreparedStatements()).isEqualTo(expected.getDeallocatedPreparedStatements());

        assertThat(actual.getStartedTransactionId()).isEqualTo(expected.getStartedTransactionId());
        assertThat(actual.isClearTransactionId()).isEqualTo(expected.isClearTransactionId());

        assertThat(actual.getUpdateType()).isEqualTo(expected.getUpdateType());
        assertThat(actual.getOutputStage()).isEqualTo(expected.getOutputStage());

        assertThat(actual.getFailureInfo()).isEqualTo(expected.getFailureInfo());
        assertThat(actual.getErrorCode()).isEqualTo(expected.getErrorCode());
        assertThat(actual.getWarnings()).isEqualTo(expected.getWarnings());

        assertThat(actual.getInputs()).isEqualTo(expected.getInputs());
        assertThat(actual.getOutput()).isEqualTo(expected.getOutput());

        assertThat(actual.getReferencedTables()).isEqualTo(expected.getReferencedTables());
        assertThat(actual.getRoutines()).isEqualTo(expected.getRoutines());

        assertThat(actual.isFinalQueryInfo()).isEqualTo(expected.isFinalQueryInfo());

        assertThat(actual.getResourceGroupId()).isEqualTo(expected.getResourceGroupId());
        assertThat(actual.getQueryType()).isEqualTo(expected.getQueryType());
        assertThat(actual.getRetryPolicy()).isEqualTo(expected.getRetryPolicy());
    }

    @Test
    public void testQueryInfoToResultQueryInfoConversion()
    {
        QueryInfo queryInfo = createQueryInfo(Optional.of(createStageInfo(1, StageState.FINISHED, 1)));
        QueryStats queryStats = queryInfo.getQueryStats();
        BasicQueryStats basicQueryStats = new BasicQueryStats(queryStats);
        StageInfo stageInfo = queryInfo.getOutputStage().get();
        BasicStageInfo basicStageInfo = new BasicStageInfo(stageInfo);

        ResultQueryInfo resultQueryInfo = new ResultQueryInfo(queryInfo);
        assertThat(queryInfo.getQueryId()).isEqualTo(resultQueryInfo.queryId());
        assertThat(queryInfo.getState()).isEqualTo(resultQueryInfo.state());
        assertThat(queryInfo.isScheduled()).isEqualTo(resultQueryInfo.scheduled());
        assertThat(queryInfo.getUpdateType()).isEqualTo(resultQueryInfo.updateType());
        assertThat(queryInfo.getErrorCode()).isEqualTo(resultQueryInfo.errorCode());
        assertThat(queryInfo.isFinalQueryInfo()).isEqualTo(resultQueryInfo.finalQueryInfo());
        assertThat(queryInfo.getFailureInfo()).isEqualTo(resultQueryInfo.failureInfo());
        assertThat(queryInfo.getSetCatalog()).isEqualTo(resultQueryInfo.setCatalog());
        assertThat(queryInfo.getSetSchema()).isEqualTo(resultQueryInfo.setSchema());
        assertThat(queryInfo.getSetPath()).isEqualTo(resultQueryInfo.setPath());
        assertThat(queryInfo.getSetAuthorizationUser()).isEqualTo(resultQueryInfo.setAuthorizationUser());
        assertThat(queryInfo.isResetAuthorizationUser()).isEqualTo(resultQueryInfo.resetAuthorizationUser());
        assertThat(queryInfo.getErrorCode()).isEqualTo(resultQueryInfo.errorCode());
        assertThat(queryInfo.getSetRoles()).isEqualTo(resultQueryInfo.setRoles());
        assertThat(queryInfo.getSetSessionProperties()).isEqualTo(resultQueryInfo.setSessionProperties());
        assertThat(queryInfo.getResetSessionProperties()).isEqualTo(resultQueryInfo.resetSessionProperties());
        assertThat(queryInfo.getAddedPreparedStatements()).isEqualTo(resultQueryInfo.addedPreparedStatements());
        assertThat(queryInfo.getDeallocatedPreparedStatements()).isEqualTo(resultQueryInfo.deallocatedPreparedStatements());
        assertThat(queryInfo.getStartedTransactionId()).isEqualTo(resultQueryInfo.startedTransactionId());
        assertThat(queryInfo.isClearTransactionId()).isEqualTo(resultQueryInfo.clearTransactionId());
        assertThat(queryInfo.getWarnings()).isEqualTo(resultQueryInfo.warnings());

        assertThat(queryStats.getCreateTime()).isEqualTo(basicQueryStats.getCreateTime());
        assertThat(queryStats.getEndTime()).isEqualTo(basicQueryStats.getEndTime());
        assertThat(queryStats.getQueuedTime()).isEqualTo(basicQueryStats.getQueuedTime());
        assertThat(queryStats.getElapsedTime()).isEqualTo(basicQueryStats.getElapsedTime());
        assertThat(queryStats.getExecutionTime()).isEqualTo(basicQueryStats.getExecutionTime());
        assertThat(queryStats.getFailedTasks()).isEqualTo(basicQueryStats.getFailedTasks());
        assertThat(queryStats.getTotalDrivers()).isEqualTo(basicQueryStats.getTotalDrivers());
        assertThat(queryStats.getQueuedDrivers()).isEqualTo(basicQueryStats.getQueuedDrivers());
        assertThat(queryStats.getCompletedDrivers()).isEqualTo(basicQueryStats.getCompletedDrivers());
        assertThat(queryStats.getRunningDrivers()).isEqualTo(basicQueryStats.getRunningDrivers());
        assertThat(queryStats.getBlockedDrivers()).isEqualTo(basicQueryStats.getBlockedDrivers());
        assertThat(queryStats.getRawInputPositions()).isEqualTo(basicQueryStats.getRawInputPositions());
        assertThat(queryStats.getRawInputDataSize()).isEqualTo(basicQueryStats.getRawInputDataSize());
        assertThat(queryStats.getPhysicalInputDataSize()).isEqualTo(basicQueryStats.getPhysicalInputDataSize());
        assertThat(queryStats.getPhysicalWrittenDataSize()).isEqualTo(basicQueryStats.getPhysicalWrittenDataSize());
        assertThat(queryStats.getSpilledDataSize()).isEqualTo(basicQueryStats.getSpilledDataSize());
        assertThat(queryStats.getCumulativeUserMemory()).isEqualTo(basicQueryStats.getCumulativeUserMemory());
        assertThat(queryStats.getFailedCumulativeUserMemory()).isEqualTo(basicQueryStats.getFailedCumulativeUserMemory());
        assertThat(queryStats.getUserMemoryReservation()).isEqualTo(basicQueryStats.getUserMemoryReservation());
        assertThat(queryStats.getTotalMemoryReservation()).isEqualTo(basicQueryStats.getTotalMemoryReservation());
        assertThat(queryStats.getPeakUserMemoryReservation()).isEqualTo(basicQueryStats.getPeakUserMemoryReservation());
        assertThat(queryStats.getPeakTotalMemoryReservation()).isEqualTo(basicQueryStats.getPeakTotalMemoryReservation());
        assertThat(queryStats.getTotalCpuTime()).isEqualTo(basicQueryStats.getTotalCpuTime());
        assertThat(queryStats.isFullyBlocked()).isEqualTo(basicQueryStats.isFullyBlocked());
        assertThat(queryStats.getTotalCpuTime()).isEqualTo(basicQueryStats.getTotalCpuTime());
        assertThat(queryStats.getFailedCpuTime()).isEqualTo(basicQueryStats.getFailedCpuTime());
        assertThat(queryStats.getTotalScheduledTime()).isEqualTo(basicQueryStats.getTotalScheduledTime());
        assertThat(queryStats.getFailedScheduledTime()).isEqualTo(basicQueryStats.getFailedScheduledTime());
        assertThat(queryStats.getBlockedReasons()).isEqualTo(basicQueryStats.getBlockedReasons());
        assertThat(queryStats.getProgressPercentage()).isEqualTo(basicQueryStats.getProgressPercentage());
        assertThat(queryStats.getRunningPercentage()).isEqualTo(basicQueryStats.getRunningPercentage());

        assertThat(stageInfo.getStageId()).isEqualTo(basicStageInfo.getStageId());
        assertThat(stageInfo.getState()).isEqualTo(basicStageInfo.getState());
        assertThat(stageInfo.isCoordinatorOnly()).isEqualTo(basicStageInfo.isCoordinatorOnly());
        assertThat(stageInfo.getTasks()).isEqualTo(basicStageInfo.getTasks());
        assertThat(stageInfo.getStageStats().getFailedTasks()).isEqualTo(basicStageInfo.getStageStats().getFailedTasks());
        assertThat(stageInfo.getStageStats().getTotalDrivers()).isEqualTo(basicStageInfo.getStageStats().getTotalDrivers());
        assertThat(stageInfo.getStageStats().getQueuedDrivers()).isEqualTo(basicStageInfo.getStageStats().getQueuedDrivers());

        assertThat(stageInfo.getStageStats().getBlockedDrivers()).isEqualTo(basicStageInfo.getStageStats().getBlockedDrivers());
        assertThat(stageInfo.getStageStats().getPhysicalWrittenDataSize()).isEqualTo(basicStageInfo.getStageStats().getPhysicalWrittenDataSize());
    }

    private static QueryInfo createQueryInfo(Optional<StageInfo> stageInfo)
    {
        return new QueryInfo(
                new QueryId("0"),
                TEST_SESSION.toSessionRepresentation(),
                FINISHED,
                URI.create("1"),
                ImmutableList.of("number"),
                "SELECT 1 as number",
                Optional.of("prepared_query"),
                TestQueryStats.EXPECTED,
                Optional.of("set_catalog"),
                Optional.of("set_schema"),
                Optional.of("set_path"),
                Optional.of("set_authorization_user"),
                false,
                ImmutableMap.of("set_property", "set_value"),
                ImmutableSet.of("reset_property"),
                ImmutableMap.of("set_roles", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("role"))),
                ImmutableMap.of("added_prepared_statement", "statement"),
                ImmutableSet.of("deallocated_prepared_statement", "statement"),
                Optional.of(TransactionId.create()),
                true,
                "42",
                stageInfo,
                null,
                null,
                ImmutableList.of(new TrinoWarning(new WarningCode(1, "name"), "message")),
                ImmutableSet.of(new Input("catalog", new CatalogVersion("default"), "schema", "talble", Optional.empty(), ImmutableList.of(new Column("name", "type")), new PlanFragmentId("id"), new PlanNodeId("1"))),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                true,
                Optional.of(new ResourceGroupId("groupId")),
                Optional.of(QueryType.SELECT),
                RetryPolicy.TASK,
                false,
                new NodeVersion("test"));
    }

    private static StageInfo createStageInfo(int count, StageState state, int baseValue)
    {
        return new StageInfo(
                StageId.valueOf(ImmutableList.of("s", String.valueOf(count))),
                state,
                null,
                false,
                ImmutableList.of(BIGINT),
                createStageStats(baseValue),
                ImmutableList.of(),
                count == 1 ? ImmutableList.of() : ImmutableList.of(createStageInfo(count - 1, state, baseValue)),
                ImmutableMap.of(),
                new ExecutionFailureInfo("", "", null, ImmutableList.of(), ImmutableList.of(), null, null, null));
    }

    private static StageStats createStageStats(int value)
    {
        return new StageStats(
                new DateTime(value),
                new Distribution.DistributionSnapshot(value, value, value, value, value, value, value, value, value, value, value, value, value, value),
                value,
                value,
                value,
                value,
                value,
                value,
                value,
                value,
                value,
                value,
                value,
                succinctBytes(value),
                succinctBytes(value),
                succinctBytes(value),
                succinctBytes(value),
                succinctBytes(value),
                Duration.succinctDuration(value, SECONDS),
                Duration.succinctDuration(value, SECONDS),
                Duration.succinctDuration(value, SECONDS),
                Duration.succinctDuration(value, SECONDS),
                Duration.succinctDuration(value, SECONDS),
                false,
                org.weakref.jmx.$internal.guava.collect.ImmutableSet.of(),
                succinctBytes(value),
                succinctBytes(value),
                value,
                value,
                Duration.succinctDuration(value, SECONDS),
                Duration.succinctDuration(value, SECONDS),
                succinctBytes(value),
                succinctBytes(value),
                value,
                value,
                succinctBytes(value),
                succinctBytes(value),
                value,
                value,
                succinctBytes(value),
                succinctBytes(value),
                value,
                value,
                Duration.succinctDuration(value, SECONDS),
                Duration.succinctDuration(value, SECONDS),
                succinctBytes(value),
                Optional.of(new TDigestHistogram(new TDigest())),
                succinctBytes(value),
                succinctBytes(value),
                value,
                value,
                Duration.succinctDuration(value, NANOSECONDS),
                Duration.succinctDuration(value, NANOSECONDS),
                succinctBytes(value),
                succinctBytes(value),
                new StageGcStatistics(
                        value,
                        value,
                        value,
                        value,
                        value,
                        value,
                        value),
                ImmutableList.of());
    }
}
