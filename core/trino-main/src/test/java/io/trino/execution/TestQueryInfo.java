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
import io.trino.client.NodeVersion;
import io.trino.operator.RetryPolicy;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoWarning;
import io.trino.spi.WarningCode;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.SelectedRole;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.transaction.TransactionId;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.QueryState.FINISHED;
import static io.trino.tracing.TracingJsonCodec.tracingJsonCodecFactory;
import static org.testng.Assert.assertEquals;

public class TestQueryInfo
{
    @Test
    public void testQueryInfoRoundTrip()
    {
        JsonCodec<QueryInfo> codec = tracingJsonCodecFactory().jsonCodec(QueryInfo.class);
        QueryInfo expected = createQueryInfo();
        QueryInfo actual = codec.fromJson(codec.toJsonBytes(expected));

        assertEquals(actual.getQueryId(), expected.getQueryId());
        // Note: SessionRepresentation.equals?
        assertEquals(actual.getState(), expected.getState());
        assertEquals(actual.isScheduled(), expected.isScheduled());
        assertEquals(actual.getProgressPercentage(), expected.getProgressPercentage());
        assertEquals(actual.getRunningPercentage(), expected.getRunningPercentage());

        assertEquals(actual.getSelf(), expected.getSelf());
        assertEquals(actual.getFieldNames(), expected.getFieldNames());
        assertEquals(actual.getQuery(), expected.getQuery());
        assertEquals(actual.getPreparedQuery(), expected.getPreparedQuery());
        // Assert all of queryStats
        TestQueryStats.assertExpectedQueryStats(actual.getQueryStats());

        assertEquals(actual.getSetCatalog(), expected.getSetCatalog());
        assertEquals(actual.getSetSchema(), expected.getSetSchema());
        assertEquals(actual.getSetPath(), expected.getSetPath());
        assertEquals(actual.getSetSessionProperties(), expected.getSetSessionProperties());
        assertEquals(actual.getResetSessionProperties(), expected.getResetSessionProperties());
        assertEquals(actual.getSetRoles(), expected.getSetRoles());
        assertEquals(actual.getAddedPreparedStatements(), expected.getAddedPreparedStatements());
        assertEquals(actual.getDeallocatedPreparedStatements(), expected.getDeallocatedPreparedStatements());

        assertEquals(actual.getStartedTransactionId(), expected.getStartedTransactionId());
        assertEquals(actual.isClearTransactionId(), expected.isClearTransactionId());

        assertEquals(actual.getUpdateType(), expected.getUpdateType());
        assertEquals(actual.getOutputStage(), expected.getOutputStage());

        assertEquals(actual.getFailureInfo(), expected.getFailureInfo());
        assertEquals(actual.getErrorCode(), expected.getErrorCode());
        assertEquals(actual.getWarnings(), expected.getWarnings());

        assertEquals(actual.getInputs(), expected.getInputs());
        assertEquals(actual.getOutput(), expected.getOutput());

        assertEquals(actual.getReferencedTables(), expected.getReferencedTables());
        assertEquals(actual.getRoutines(), expected.getRoutines());

        assertEquals(actual.isFinalQueryInfo(), expected.isFinalQueryInfo());

        assertEquals(actual.getResourceGroupId(), expected.getResourceGroupId());
        assertEquals(actual.getQueryType(), expected.getQueryType());
        assertEquals(actual.getRetryPolicy(), expected.getRetryPolicy());
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
