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
import io.trino.spi.block.IntArrayBlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.TestingTypeManager;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.pinot.MetadataUtil.TEST_TABLE;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPinotMetadata
{
    private final PinotConfig pinotConfig = new PinotConfig().setControllerUrls(ImmutableList.of("localhost:9000"));
    private final PinotMetadata metadata = new PinotMetadata(new MockPinotClient(pinotConfig), pinotConfig, Executors.newSingleThreadExecutor(), new PinotTypeConverter(new TestingTypeManager()));

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
    public void testBuildConstraintSQLThrowsUnsupportedOperationException()
    {
        assertThatThrownBy(() ->
                PinotMetadata.buildConstraintPql(
                        new Call(BOOLEAN, new FunctionName("unsupported"), List.of()), null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testBuildConstraintPqlEqualsNotEqualsPredicate()
    {
        assertPQLEquals(getCall("$equal", "string"), "string = 'string'");
        assertPQLEquals(getCall("$equal", "int"), "int = 1");
        assertPQLEquals(getCall("$not_equal", "string"), "string != 'string'");
        assertPQLEquals(getCall("$not_equal", "int"), "int != 1");
    }

    @Test
    public void testBuildConstraintPqlJsonExtractIsNullPredicate()
    {
        assertPQLEquals(getCall("$is_null", "json_extract_scalar"),
                "JSON_MATCH(json, '\"$.selector\" IS NULL')");
    }

    @Test
    public void testBuildConstraintPqlJsonContainsPredicate()
    {
        assertPQLEquals(getCall("contains", "string_array"),
                "JSON_MATCH(json, '\"$.selector\" IN (''a'',''b'',''c'')')");
        assertPQLEquals(getCall("contains", "int_array"),
                "JSON_MATCH(json, '\"$.selector\" IN (1)')");
    }

    @Test
    public void testBuildConstraintPqlPinotJsonArrayContainsPredicate()
    {
        assertPQLEquals(getCall("json_array_contains", "array_contains_string"),
                "JSON_MATCH(json, '\"$.selector[*]\" = ''string''')");
        assertPQLEquals(getCall("json_array_contains", "array_contains_int"),
                "JSON_MATCH(json, '\"$.selector[*]\" = 1')");
    }

    @Test
    public void testBuildConstraintPqlPinotJsonArrayContainsEqualsPredicate()
    {
        // equals false case
        assertPQLEquals(new Call(BOOLEAN, new FunctionName("$equal"), List.of(
                        getCall("json_array_contains", "array_contains_string"),
                        new Constant(false, BOOLEAN))),
                "NOT(JSON_MATCH(json, '\"$.selector[*]\" = ''string'''))");

        // equals true case
        assertPQLEquals(new Call(BOOLEAN, new FunctionName("$equal"), List.of(
                        getCall("json_array_contains", "array_contains_string"),
                        new Constant(true, BOOLEAN))),
                "JSON_MATCH(json, '\"$.selector[*]\" = ''string''')");

        // not equals true case
        assertPQLEquals(new Call(BOOLEAN, new FunctionName("$not_equal"), List.of(
                        getCall("json_array_contains", "array_contains_string"),
                        new Constant(true, BOOLEAN))),
                "NOT(JSON_MATCH(json, '\"$.selector[*]\" = ''string'''))");

        // not equals false case
        assertPQLEquals(new Call(BOOLEAN, new FunctionName("$not_equal"), List.of(
                        getCall("json_array_contains", "array_contains_string"),
                        new Constant(false, BOOLEAN))),
                "JSON_MATCH(json, '\"$.selector[*]\" = ''string''')");
    }

    @Test
    public void testBuildConstraintPqlPinotNotJsonArrayContainsPredicate()
    {
        assertPQLEquals(new Call(BOOLEAN, new FunctionName("$not"), List.of(
                        getCall("json_array_contains", "array_contains_string"))),
                "NOT(JSON_MATCH(json, '\"$.selector[*]\" = ''string'''))");
    }

    @Test
    public void testBuildConstraintPqlPinotJsonContainsEqualsPredicate()
    {
        // equals false case
        Call containsCall = getCall("contains", "string_array");
        assertPQLEquals(new Call(BOOLEAN, new FunctionName("$equal"), List.of(
                containsCall,
                new Constant(false, BOOLEAN))),
                "JSON_MATCH(json, '\"$.selector\" NOT IN (''a'',''b'',''c'')')");

        // equals true case
        assertPQLEquals(new Call(BOOLEAN, new FunctionName("$equal"), List.of(
                        containsCall,
                        new Constant(true, BOOLEAN))),
                "JSON_MATCH(json, '\"$.selector\" IN (''a'',''b'',''c'')')");

        // not equals true case
        assertPQLEquals(new Call(BOOLEAN, new FunctionName("$not_equal"), List.of(
                        containsCall,
                        new Constant(true, BOOLEAN))),
                "JSON_MATCH(json, '\"$.selector\" NOT IN (''a'',''b'',''c'')')");

        // not equals false case
        assertPQLEquals(new Call(BOOLEAN, new FunctionName("$not_equal"), List.of(
                        containsCall,
                        new Constant(false, BOOLEAN))),
                "JSON_MATCH(json, '\"$.selector\" IN (''a'',''b'',''c'')')");
    }

    @Test
    public void testBuildConstraintPqlPinotNotJsonContainsPredicate()
    {
        assertPQLEquals(new Call(BOOLEAN, new FunctionName("$not"), List.of(
                getCall("contains", "string_array"))),
                "JSON_MATCH(json, '\"$.selector\" NOT IN (''a'',''b'',''c'')')");
    }

    @Test
    public void testBuildConstraintPQLANDedPredicates()
    {
        assertPQLEquals(new Call(BOOLEAN, new FunctionName("$and"), List.of(
                        getCall("$equal", "string"),
                        getCall("$is_null", "json_extract_scalar"))),
                "(string = 'string' AND JSON_MATCH(json, '\"$.selector\" IS NULL'))");
    }

    @Test
    public void testBuildConstraintPQLORedPredicates()
    {
        assertPQLEquals(new Call(BOOLEAN, new FunctionName("$or"), List.of(
                        getCall("$equal", "string"),
                        getCall("$is_null", "json_extract_scalar"))),
                "(string = 'string' OR JSON_MATCH(json, '\"$.selector\" IS NULL'))");
    }

    @Test
    public void testBuildConstraintPQLNOTedExpression()
    {
        assertPQLEquals(new Call(BOOLEAN, new FunctionName("$not"), List.of(
                new Call(BOOLEAN, new FunctionName("$or"), List.of(
                        getCall("$equal", "string"),
                        getCall("$is_null", "json_extract_scalar"))))),
                "NOT((string = 'string' OR JSON_MATCH(json, '\"$.selector\" IS NULL')))");
    }

    private static Call getCall(String functionName, String argType)
    {
        List<ConnectorExpression> arguments = switch (argType) {
            case "string" -> List.of(
                    new Variable("string", VARCHAR),
                    new Constant(utf8Slice("string"), VARCHAR));
            case "int" -> List.of(
                    new Variable("int", VARCHAR),
                    new Constant(1, INTEGER));
            case "json_extract", "json_extract_scalar" -> List.of(
                    new Call(BOOLEAN, new FunctionName(argType), List.of(
                            new Variable("json", VARCHAR),
                            new Constant(utf8Slice("$.selector"), VARCHAR))));
            case "string_array" -> List.of(
                    new Constant(new VariableWidthBlockBuilder(null, 0, 0)
                            .writeEntry(utf8Slice("a"))
                            .writeEntry(utf8Slice("b"))
                            .writeEntry(utf8Slice("c"))
                            .buildValueBlock(), VARCHAR),
                    new Call(BOOLEAN, new FunctionName("json_extract_scalar"), List.of(
                            new Variable("json", VARCHAR),
                            new Constant(utf8Slice("$.selector"), VARCHAR))));
            case "int_array" -> List.of(
                    new Constant(new IntArrayBlockBuilder(null, 0)
                            .writeInt(1)
                            .build(), INTEGER),
                    getCall("$cast", "json_extract_scalar"));
            case "array_contains_string" -> List.of(
                    new Call(BOOLEAN, new FunctionName("json_extract"), List.of(
                            new Variable("json", VARCHAR),
                            new Constant(utf8Slice("$.selector"), VARCHAR))),
                    new Constant(utf8Slice("string"), VARCHAR));
            case "array_contains_int" -> List.of(
                    new Call(BOOLEAN, new FunctionName("json_extract"), List.of(
                            new Variable("json", VARCHAR),
                            new Constant(utf8Slice("$.selector"), VARCHAR))),
                    new Constant(1, INTEGER));
            default -> List.of();
        };
        return new Call(BOOLEAN, new FunctionName(functionName), arguments);
    }

    private static void assertPQLEquals(Call call, String pql)
    {
        StringBuilder pqlBuilder = new StringBuilder();
        PinotMetadata.buildConstraintPql(call, pqlBuilder);
        assertThat(pqlBuilder.toString()).isEqualTo(pql);
    }
}
