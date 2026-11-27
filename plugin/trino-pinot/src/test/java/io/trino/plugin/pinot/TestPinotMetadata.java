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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.pinot.MetadataUtil.TEST_TABLE;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPinotMetadata
{
    private final PinotConfig pinotConfig = new PinotConfig().setControllerUrls(ImmutableList.of("localhost:9000"));
    private final PinotMetadata metadata = new PinotMetadata(new MockPinotClient(pinotConfig), pinotConfig, Executors.newSingleThreadExecutor(), new PinotTypeConverter(TESTING_TYPE_MANAGER));

    @Test
    public void testTables()
    {
        ConnectorSession session = TestPinotSplitManager.createSessionWithNumSplits(1, false, pinotConfig);
        List<SchemaTableName> schemaTableNames = metadata.listTables(session, Optional.empty());
        assertThat(ImmutableSet.copyOf(schemaTableNames)).isEqualTo(ImmutableSet.builder()
                .add(new SchemaTableName("default", TestPinotSplitManager.realtimeOnlyTable.tableName()))
                .add(new SchemaTableName("default", TestPinotSplitManager.hybridTable.tableName()))
                .add(new SchemaTableName("default", TEST_TABLE))
                .build());
        List<String> schemas = metadata.listSchemaNames(session);
        assertThat(ImmutableList.copyOf(schemas)).isEqualTo(ImmutableList.of("default"));
        PinotTableHandle withWeirdSchema = metadata.getTableHandle(
                session,
                new SchemaTableName("foo", TestPinotSplitManager.realtimeOnlyTable.tableName()),
                Optional.empty(),
                Optional.empty());
        assertThat(withWeirdSchema.tableName()).isEqualTo(TestPinotSplitManager.realtimeOnlyTable.tableName());
        PinotTableHandle withAnotherSchema = metadata.getTableHandle(
                session,
                new SchemaTableName(TestPinotSplitManager.realtimeOnlyTable.tableName(), TestPinotSplitManager.realtimeOnlyTable.tableName()),
                Optional.empty(),
                Optional.empty());
        assertThat(withAnotherSchema.tableName()).isEqualTo(TestPinotSplitManager.realtimeOnlyTable.tableName());
        PinotTableHandle withUppercaseTable = metadata.getTableHandle(
                session,
                new SchemaTableName("default", TEST_TABLE),
                Optional.empty(),
                Optional.empty());
        assertThat(withUppercaseTable.tableName()).isEqualTo("airlineStats");
    }

    @Test
    public void testApplyFilterPushesJsonPredicate()
    {
        ConnectorExpression isNull = jsonIsNull("$.a");
        ConstraintApplicationResult<ConnectorTableHandle> result = applyFilter(true, isNull).orElseThrow();

        assertThat(((PinotTableHandle) result.getHandle()).constraintPql())
                .contains("JSON_MATCH(\"json\", '\"$.a\" IS NULL')");
        assertThat(result.getRemainingExpression()).contains(Constant.TRUE);
    }

    @Test
    public void testApplyFilterKeepsUnsupportedConjunct()
    {
        ConnectorExpression unsupported = new Call(BOOLEAN, new FunctionName("starts_with"), List.of(new Variable("json", VARCHAR), new Constant(utf8Slice("v"), VARCHAR)));
        ConstraintApplicationResult<ConnectorTableHandle> result = applyFilter(true, jsonIsNull("$.a"), unsupported).orElseThrow();

        assertThat(((PinotTableHandle) result.getHandle()).constraintPql()).isPresent();
        assertThat(result.getRemainingExpression()).contains(unsupported);
    }

    @Test
    public void testApplyFilterRechecksNegatedPredicate()
    {
        // a missing JSON value is false for Pinot but NULL for Trino, so NOT(...) must be evaluated again by Trino
        ConnectorExpression negated = new Call(BOOLEAN, NOT_FUNCTION_NAME, List.of(jsonIsNull("$.a")));
        ConstraintApplicationResult<ConnectorTableHandle> result = applyFilter(true, negated).orElseThrow();

        assertThat(((PinotTableHandle) result.getHandle()).constraintPql()).isPresent();
        assertThat(result.getRemainingExpression()).contains(negated);
    }

    @Test
    public void testApplyFilterDoesNotPushMultipleNegations()
    {
        ConnectorExpression negated = new Call(BOOLEAN, NOT_FUNCTION_NAME, List.of(new Call(BOOLEAN, NOT_FUNCTION_NAME, List.of(jsonIsNull("$.a")))));
        assertThat(applyFilter(true, negated)).isEmpty();
    }

    @Test
    public void testApplyFilterDisabledByDefault()
    {
        assertThat(applyFilter(false, jsonIsNull("$.a"))).isEmpty();
    }

    private Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(boolean jsonPushdown, ConnectorExpression... conjuncts)
    {
        PinotConfig config = new PinotConfig()
                .setControllerUrls(ImmutableList.of("localhost:9000"))
                .setJsonPredicatePushdownEnabled(jsonPushdown);
        ConnectorSession session = TestPinotSplitManager.createSessionWithNumSplits(1, false, config);
        ConnectorExpression expression = ConnectorExpressions.and(List.of(conjuncts));
        Constraint constraint = new Constraint(TupleDomain.all(), expression, Map.of("json", new PinotColumnHandle("json", VARCHAR)));
        return metadata.applyFilter(session, TestPinotTableHandle.newTableHandle("default", "airlineStats"), constraint);
    }

    private static ConnectorExpression jsonIsNull(String path)
    {
        Call extract = new Call(VARCHAR, new FunctionName("json_extract_scalar"), List.of(new Variable("json", VARCHAR), new Constant(utf8Slice(path), VARCHAR)));
        return new Call(BOOLEAN, IS_NULL_FUNCTION_NAME, List.of(extract));
    }
}
