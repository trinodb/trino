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
import io.airlift.tracing.SpanSerialization.SpanDeserializer;
import io.airlift.tracing.SpanSerialization.SpanSerializer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.trino.client.NodeVersion;
import io.trino.operator.RetryPolicy;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoWarning;
import io.trino.spi.WarningCode;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.transaction.TransactionId;
import io.trino.type.TypeSignatureDeserializer;
import io.trino.type.TypeSignatureKeyDeserializer;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.QueryState.FINISHED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryInfo
{
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
                                TypeSignature.class, new TypeSignatureKeyDeserializer())))
                .jsonCodec(QueryInfo.class);

        QueryInfo expected = createQueryInfo();
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

    private static QueryInfo createQueryInfo()
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
                Optional.empty(),
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
}
