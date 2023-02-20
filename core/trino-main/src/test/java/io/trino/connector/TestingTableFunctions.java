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
package io.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.HostAddress;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.ptf.AbstractConnectorTableFunction;
import io.trino.spi.ptf.Argument;
import io.trino.spi.ptf.ConnectorTableFunctionHandle;
import io.trino.spi.ptf.Descriptor;
import io.trino.spi.ptf.DescriptorArgumentSpecification;
import io.trino.spi.ptf.ReturnTypeSpecification.DescribedTable;
import io.trino.spi.ptf.ScalarArgument;
import io.trino.spi.ptf.ScalarArgumentSpecification;
import io.trino.spi.ptf.TableArgument;
import io.trino.spi.ptf.TableArgumentSpecification;
import io.trino.spi.ptf.TableFunctionAnalysis;
import io.trino.spi.ptf.TableFunctionDataProcessor;
import io.trino.spi.ptf.TableFunctionProcessState;
import io.trino.spi.ptf.TableFunctionProcessState.TableFunctionResult;
import io.trino.spi.ptf.TableFunctionProcessorProvider;
import io.trino.spi.ptf.TableFunctionSplitProcessor;
import io.trino.spi.type.RowType;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.connector.TestingTableFunctions.ConstantFunction.ConstantFunctionSplit.DEFAULT_SPLIT_SIZE;
import static io.trino.spi.ptf.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.ptf.ReturnTypeSpecification.OnlyPassThrough.ONLY_PASS_THROUGH;
import static io.trino.spi.ptf.TableFunctionProcessState.Type.FINISHED;
import static io.trino.spi.ptf.TableFunctionProcessState.Type.FORWARD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class TestingTableFunctions
{
    private static final String SCHEMA_NAME = "system";
    private static final String TABLE_NAME = "table";
    private static final String COLUMN_NAME = "column";
    private static final ConnectorTableFunctionHandle HANDLE = new TestingTableFunctionHandle(TEST_CATALOG_HANDLE);
    private static final TableFunctionAnalysis ANALYSIS = TableFunctionAnalysis.builder()
            .handle(HANDLE)
            .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field(COLUMN_NAME, Optional.of(BOOLEAN)))))
            .build();
    private static final TableFunctionAnalysis NO_DESCRIPTOR_ANALYSIS = TableFunctionAnalysis.builder()
            .handle(HANDLE)
            .requiredColumns("INPUT", ImmutableList.of(0))
            .build();

    /**
     * A table function returning a table with single empty column of type BOOLEAN.
     * The argument `COLUMN` is the column name.
     * The argument `IGNORED` is ignored.
     * Both arguments are optional.
     */
    public static class SimpleTableFunction
            extends AbstractConnectorTableFunction
    {
        private static final String FUNCTION_NAME = "simple_table_function";
        private static final String TABLE_NAME = "simple_table";

        public SimpleTableFunction()
        {
            super(
                    SCHEMA_NAME,
                    FUNCTION_NAME,
                    List.of(ScalarArgumentSpecification.builder()
                                    .name("COLUMN")
                                    .type(VARCHAR)
                                    .defaultValue(utf8Slice("col"))
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("IGNORED")
                                    .type(BIGINT)
                                    .defaultValue(0L)
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            ScalarArgument argument = (ScalarArgument) arguments.get("COLUMN");
            String columnName = ((Slice) argument.getValue()).toStringUtf8();

            return TableFunctionAnalysis.builder()
                    .handle(new SimpleTableFunctionHandle(getSchema(), catalogHandle, TABLE_NAME, columnName))
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field(columnName, Optional.of(BOOLEAN)))))
                    .build();
        }

        public static class SimpleTableFunctionHandle
                implements ConnectorTableFunctionHandle
        {
            private final CatalogHandle catalogHandle;
            private final MockConnectorTableHandle tableHandle;

            public SimpleTableFunctionHandle(String schema, CatalogHandle catalogHandle, String table, String column)
            {
                this.catalogHandle = catalogHandle;
                this.tableHandle = new MockConnectorTableHandle(
                        new SchemaTableName(schema, table),
                        TupleDomain.all(),
                        Optional.of(ImmutableList.of(new MockConnectorColumnHandle(column, BOOLEAN))));
            }

            @Override
            @JsonProperty
            public CatalogHandle getCatalogHandle()
            {
                return catalogHandle;
            }

            public MockConnectorTableHandle getTableHandle()
            {
                return tableHandle;
            }
        }
    }

    public static class TwoScalarArgumentsFunction
            extends AbstractConnectorTableFunction
    {
        public TwoScalarArgumentsFunction()
        {
            super(
                    SCHEMA_NAME,
                    "two_arguments_function",
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder()
                                    .name("TEXT")
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("NUMBER")
                                    .type(BIGINT)
                                    .defaultValue(null)
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return ANALYSIS;
        }
    }

    public static class TableArgumentFunction
            extends AbstractConnectorTableFunction
    {
        public TableArgumentFunction()
        {
            super(
                    SCHEMA_NAME,
                    "table_argument_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .keepWhenEmpty()
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(HANDLE)
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field(COLUMN_NAME, Optional.of(BOOLEAN)))))
                    .requiredColumns("INPUT", ImmutableList.of(0))
                    .build();
        }
    }

    public static class TableArgumentRowSemanticsFunction
            extends AbstractConnectorTableFunction
    {
        public TableArgumentRowSemanticsFunction()
        {
            super(
                    SCHEMA_NAME,
                    "table_argument_row_semantics_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .rowSemantics()
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(HANDLE)
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field(COLUMN_NAME, Optional.of(BOOLEAN)))))
                    .requiredColumns("INPUT", ImmutableList.of(0))
                    .build();
        }
    }

    public static class DescriptorArgumentFunction
            extends AbstractConnectorTableFunction
    {
        public DescriptorArgumentFunction()
        {
            super(
                    SCHEMA_NAME,
                    "descriptor_argument_function",
                    ImmutableList.of(
                            DescriptorArgumentSpecification.builder()
                                    .name("SCHEMA")
                                    .defaultValue(null)
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return ANALYSIS;
        }
    }

    public static class TwoTableArgumentsFunction
            extends AbstractConnectorTableFunction
    {
        public TwoTableArgumentsFunction()
        {
            super(
                    SCHEMA_NAME,
                    "two_table_arguments_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT1")
                                    .keepWhenEmpty()
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT2")
                                    .keepWhenEmpty()
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(HANDLE)
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field(COLUMN_NAME, Optional.of(BOOLEAN)))))
                    .requiredColumns("INPUT1", ImmutableList.of(0))
                    .requiredColumns("INPUT2", ImmutableList.of(0))
                    .build();
        }
    }

    public static class OnlyPassThroughFunction
            extends AbstractConnectorTableFunction
    {
        public OnlyPassThroughFunction()
        {
            super(
                    SCHEMA_NAME,
                    "only_pass_through_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .passThroughColumns()
                                    .keepWhenEmpty()
                                    .build()),
                    ONLY_PASS_THROUGH);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return NO_DESCRIPTOR_ANALYSIS;
        }
    }

    public static class MonomorphicStaticReturnTypeFunction
            extends AbstractConnectorTableFunction
    {
        public MonomorphicStaticReturnTypeFunction()
        {
            super(
                    SCHEMA_NAME,
                    "monomorphic_static_return_type_function",
                    ImmutableList.of(),
                    new DescribedTable(Descriptor.descriptor(
                            ImmutableList.of("a", "b"),
                            ImmutableList.of(BOOLEAN, INTEGER))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(HANDLE)
                    .build();
        }
    }

    public static class PolymorphicStaticReturnTypeFunction
            extends AbstractConnectorTableFunction
    {
        public PolymorphicStaticReturnTypeFunction()
        {
            super(
                    SCHEMA_NAME,
                    "polymorphic_static_return_type_function",
                    ImmutableList.of(TableArgumentSpecification.builder()
                            .name("INPUT")
                            .keepWhenEmpty()
                            .build()),
                    new DescribedTable(Descriptor.descriptor(
                            ImmutableList.of("a", "b"),
                            ImmutableList.of(BOOLEAN, INTEGER))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return NO_DESCRIPTOR_ANALYSIS;
        }
    }

    public static class PassThroughFunction
            extends AbstractConnectorTableFunction
    {
        public PassThroughFunction()
        {
            super(
                    SCHEMA_NAME,
                    "pass_through_function",
                    ImmutableList.of(TableArgumentSpecification.builder()
                            .name("INPUT")
                            .passThroughColumns()
                            .keepWhenEmpty()
                            .build()),
                    new DescribedTable(Descriptor.descriptor(
                            ImmutableList.of("x"),
                            ImmutableList.of(BOOLEAN))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return NO_DESCRIPTOR_ANALYSIS;
        }
    }

    public static class DifferentArgumentTypesFunction
            extends AbstractConnectorTableFunction
    {
        public DifferentArgumentTypesFunction()
        {
            super(
                    SCHEMA_NAME,
                    "different_arguments_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT_1")
                                    .passThroughColumns()
                                    .keepWhenEmpty()
                                    .build(),
                            DescriptorArgumentSpecification.builder()
                                    .name("LAYOUT")
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_2")
                                    .rowSemantics()
                                    .passThroughColumns()
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("ID")
                                    .type(BIGINT)
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_3")
                                    .pruneWhenEmpty()
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(HANDLE)
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field(COLUMN_NAME, Optional.of(BOOLEAN)))))
                    .requiredColumns("INPUT_1", ImmutableList.of(0))
                    .requiredColumns("INPUT_2", ImmutableList.of(0))
                    .requiredColumns("INPUT_3", ImmutableList.of(0))
                    .build();
        }
    }

    public static class RequiredColumnsFunction
            extends AbstractConnectorTableFunction
    {
        public RequiredColumnsFunction()
        {
            super(
                    SCHEMA_NAME,
                    "required_columns_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .keepWhenEmpty()
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(HANDLE)
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field("column", Optional.of(BOOLEAN)))))
                    .requiredColumns("INPUT", ImmutableList.of(0, 1))
                    .build();
        }
    }

    public static class TestingTableFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final CatalogHandle catalogHandle;
        private final MockConnectorTableHandle tableHandle;

        public TestingTableFunctionHandle(CatalogHandle catalogHandle)
        {
            this.catalogHandle = catalogHandle;
            this.tableHandle = new MockConnectorTableHandle(
                    new SchemaTableName(SCHEMA_NAME, TABLE_NAME),
                    TupleDomain.all(),
                    Optional.of(ImmutableList.of(new MockConnectorColumnHandle(COLUMN_NAME, BOOLEAN))));
        }

        @Override
        @JsonProperty
        public CatalogHandle getCatalogHandle()
        {
            return catalogHandle;
        }

        public MockConnectorTableHandle getTableHandle()
        {
            return tableHandle;
        }
    }

    // for testing execution by operator

    public static class IdentityFunction
            extends AbstractConnectorTableFunction
    {
        public IdentityFunction()
        {
            super(
                    SCHEMA_NAME,
                    "identity_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .keepWhenEmpty()
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            List<RowType.Field> inputColumns = ((TableArgument) arguments.get("INPUT")).getRowType().getFields();
            Descriptor returnedType = new Descriptor(inputColumns.stream()
                    .map(field -> new Descriptor.Field(field.getName().orElse("anonymous_column"), Optional.of(field.getType())))
                    .collect(toImmutableList()));
            return TableFunctionAnalysis.builder()
                    .handle(new EmptyTableFunctionHandle())
                    .returnedType(returnedType)
                    .requiredColumns("INPUT", IntStream.range(0, inputColumns.size()).boxed().collect(toImmutableList()))
                    .build();
        }

        public static class IdentityFunctionProcessorProvider
                implements TableFunctionProcessorProvider
        {
            @Override
            public TableFunctionDataProcessor getDataProcessor(ConnectorTableFunctionHandle handle)
            {
                return input -> {
                    if (input == null) {
                        return new TableFunctionProcessState(FINISHED, false, null, null);
                    }
                    Optional<Page> inputPage = getOnlyElement(input);
                    return inputPage.map(page -> new TableFunctionProcessState(FORWARD, true, new TableFunctionResult(page), null)).orElseThrow();
                };
            }
        }
    }

    public static class IdentityPassThroughFunction
            extends AbstractConnectorTableFunction
    {
        public IdentityPassThroughFunction()
        {
            super(
                    SCHEMA_NAME,
                    "identity_pass_through_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .passThroughColumns()
                                    .keepWhenEmpty()
                                    .build()),
                    ONLY_PASS_THROUGH);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new EmptyTableFunctionHandle())
                    .requiredColumns("INPUT", ImmutableList.of(0)) // per spec, function must require at least one column
                    .build();
        }

        public static class IdentityPassThroughFunctionProcessorProvider
                implements TableFunctionProcessorProvider
        {
            @Override
            public TableFunctionDataProcessor getDataProcessor(ConnectorTableFunctionHandle handle)
            {
                return new IdentityPassThroughFunctionProcessor();
            }
        }

        public static class IdentityPassThroughFunctionProcessor
                implements TableFunctionDataProcessor
        {
            private long processedPositions; // stateful

            @Override
            public TableFunctionProcessState process(List<Optional<Page>> input)
            {
                if (input == null) {
                    return new TableFunctionProcessState(FINISHED, false, null, null);
                }

                Page page = getOnlyElement(input).orElseThrow();
                BlockBuilder builder = BIGINT.createBlockBuilder(null, page.getPositionCount());
                for (long index = processedPositions; index < processedPositions + page.getPositionCount(); index++) {
                    // TODO check for long overflow
                    builder.writeLong(index);
                }
                processedPositions = processedPositions + page.getPositionCount();
                return new TableFunctionProcessState(FORWARD, true, new TableFunctionResult(new Page(builder.build())), null);
            }
        }
    }

    public static class RepeatFunction
            extends AbstractConnectorTableFunction
    {
        public RepeatFunction()
        {
            super(
                    SCHEMA_NAME,
                    "repeat",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .passThroughColumns()
                                    .keepWhenEmpty()
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("N")
                                    .type(INTEGER)
                                    .defaultValue(2L)
                                    .build()),
                    ONLY_PASS_THROUGH);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            ScalarArgument count = (ScalarArgument) arguments.get("N");
            requireNonNull(count.getValue(), "count value for function repeat() is null");
            checkArgument((long) count.getValue() > 0, "count value for function repeat() must be positive");

            return TableFunctionAnalysis.builder()
                    .handle(new RepeatFunctionHandle(catalogHandle, (long) count.getValue()))
                    .requiredColumns("INPUT", ImmutableList.of(0)) // per spec, function must require at least one column
                    .build();
        }

        public static class RepeatFunctionHandle
                implements ConnectorTableFunctionHandle
        {
            private final CatalogHandle catalogHandle;
            private final long count;

            @JsonCreator
            public RepeatFunctionHandle(@JsonProperty("catalogHandle") CatalogHandle catalogHandle, @JsonProperty("count") long count)
            {
                this.catalogHandle = catalogHandle;
                this.count = count;
            }

            @Override
            @JsonProperty
            public CatalogHandle getCatalogHandle()
            {
                return catalogHandle;
            }

            @JsonProperty
            public long getCount()
            {
                return count;
            }
        }

        public static class RepeatFunctionProcessorProvider
                implements TableFunctionProcessorProvider
        {
            @Override
            public TableFunctionDataProcessor getDataProcessor(ConnectorTableFunctionHandle handle)
            {
                return new RepeatFunctionProcessor(((RepeatFunctionHandle) handle).getCount());
            }
        }

        public static class RepeatFunctionProcessor
                implements TableFunctionDataProcessor
        {
            private final long count;

            // stateful
            private long processedPositions;
            private long processedRounds;
            private Block indexes;
            boolean usedData;

            public RepeatFunctionProcessor(long count)
            {
                this.count = count;
            }

            @Override
            public TableFunctionProcessState process(List<Optional<Page>> input)
            {
                if (input == null) {
                    if (processedRounds < count && indexes != null) {
                        processedRounds++;
                        return new TableFunctionProcessState(FORWARD, false, new TableFunctionResult(new Page(indexes)), null);
                    }
                    return new TableFunctionProcessState(FINISHED, false, null, null);
                }

                Page page = getOnlyElement(input).orElseThrow();
                if (processedRounds == 0) {
                    BlockBuilder builder = BIGINT.createBlockBuilder(null, page.getPositionCount());
                    for (long index = processedPositions; index < processedPositions + page.getPositionCount(); index++) {
                        // TODO check for long overflow
                        builder.writeLong(index);
                    }
                    processedPositions = processedPositions + page.getPositionCount();
                    indexes = builder.build();
                    usedData = true;
                }
                else {
                    usedData = false;
                }
                processedRounds++;

                TableFunctionProcessState result = new TableFunctionProcessState(FORWARD, usedData, new TableFunctionResult(new Page(indexes)), null);

                if (processedRounds == count) {
                    processedRounds = 0;
                    indexes = null;
                }

                return result;
            }
        }
    }

    public static class TestInputsFunction
            extends AbstractConnectorTableFunction
    {
        public TestInputsFunction()
        {
            super(
                    SCHEMA_NAME,
                    "test_inputs_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .rowSemantics()
                                    .name("INPUT_1")
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_2")
                                    .keepWhenEmpty()
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_3")
                                    .keepWhenEmpty()
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_4")
                                    .keepWhenEmpty()
                                    .build()),
                    new DescribedTable(new Descriptor(ImmutableList.of(new Descriptor.Field("boolean_result", Optional.of(BOOLEAN))))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new EmptyTableFunctionHandle())
                    .requiredColumns("INPUT_1", IntStream.range(0, ((TableArgument) arguments.get("INPUT_1")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .requiredColumns("INPUT_2", IntStream.range(0, ((TableArgument) arguments.get("INPUT_2")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .requiredColumns("INPUT_3", IntStream.range(0, ((TableArgument) arguments.get("INPUT_3")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .requiredColumns("INPUT_4", IntStream.range(0, ((TableArgument) arguments.get("INPUT_4")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .build();
        }

        public static class TestInputsFunctionProcessorProvider
                implements TableFunctionProcessorProvider
        {
            @Override
            public TableFunctionDataProcessor getDataProcessor(ConnectorTableFunctionHandle handle)
            {
                BlockBuilder resultBuilder = BOOLEAN.createBlockBuilder(null, 1);
                BOOLEAN.writeBoolean(resultBuilder, true);

                TableFunctionResult result = new TableFunctionResult(new Page(resultBuilder.build()));

                return input -> {
                    if (input == null) {
                        return new TableFunctionProcessState(FINISHED, false, null, null);
                    }
                    return new TableFunctionProcessState(FORWARD, true, result, null);
                };
            }
        }
    }

    public static class PassThroughInputFunction
            extends AbstractConnectorTableFunction
    {
        public PassThroughInputFunction()
        {
            super(
                    SCHEMA_NAME,
                    "pass_through",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT_1")
                                    .passThroughColumns()
                                    .keepWhenEmpty()
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_2")
                                    .passThroughColumns()
                                    .keepWhenEmpty()
                                    .build()),
                    new DescribedTable(new Descriptor(ImmutableList.of(
                            new Descriptor.Field("input_1_present", Optional.of(BOOLEAN)),
                            new Descriptor.Field("input_2_present", Optional.of(BOOLEAN))))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new EmptyTableFunctionHandle())
                    .requiredColumns("INPUT_1", ImmutableList.of(0))
                    .requiredColumns("INPUT_2", ImmutableList.of(0))
                    .build();
        }

        public static class PassThroughInputProcessorProvider
                implements TableFunctionProcessorProvider
        {
            @Override
            public TableFunctionDataProcessor getDataProcessor(ConnectorTableFunctionHandle handle)
            {
                return new PassThroughInputProcessor();
            }
        }

        private static class PassThroughInputProcessor
                implements TableFunctionDataProcessor
        {
            private boolean input1Present;
            private boolean input2Present;
            private int input1EndIndex;
            private int input2EndIndex;
            private boolean finished;

            @Override
            public TableFunctionProcessState process(List<Optional<Page>> input)
            {
                if (finished) {
                    return new TableFunctionProcessState(FINISHED, false, null, null);
                }
                if (input == null) {
                    finished = true;

                    // proper column input_1_present
                    BlockBuilder input1Builder = BOOLEAN.createBlockBuilder(null, 1);
                    BOOLEAN.writeBoolean(input1Builder, input1Present);

                    // proper column input_2_present
                    BlockBuilder input2Builder = BOOLEAN.createBlockBuilder(null, 1);
                    BOOLEAN.writeBoolean(input2Builder, input2Present);

                    // pass-through index for input_1
                    BlockBuilder input1PassThroughBuilder = BIGINT.createBlockBuilder(null, 1);
                    if (input1Present) {
                        input1PassThroughBuilder.writeLong(input1EndIndex - 1);
                    }
                    else {
                        input1PassThroughBuilder.appendNull();
                    }

                    // pass-through index for input_2
                    BlockBuilder input2PassThroughBuilder = BIGINT.createBlockBuilder(null, 1);
                    if (input2Present) {
                        input2PassThroughBuilder.writeLong(input2EndIndex - 1);
                    }
                    else {
                        input2PassThroughBuilder.appendNull();
                    }

                    return new TableFunctionProcessState(
                            FORWARD,
                            false,
                            new TableFunctionResult(new Page(input1Builder.build(), input2Builder.build(), input1PassThroughBuilder.build(), input2PassThroughBuilder.build())),
                            null);
                }
                input.get(0).ifPresent(page -> {
                    input1Present = true;
                    input1EndIndex += page.getPositionCount();
                });
                input.get(1).ifPresent(page -> {
                    input2Present = true;
                    input2EndIndex += page.getPositionCount();
                });
                return new TableFunctionProcessState(FORWARD, true, null, null);
            }
        }
    }

    public static class TestInputFunction
            extends AbstractConnectorTableFunction
    {
        public TestInputFunction()
        {
            super(
                    SCHEMA_NAME,
                    "test_input",
                    ImmutableList.of(TableArgumentSpecification.builder()
                            .name("INPUT")
                            .keepWhenEmpty()
                            .build()),
                    new DescribedTable(new Descriptor(ImmutableList.of(new Descriptor.Field("got_input", Optional.of(BOOLEAN))))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new EmptyTableFunctionHandle())
                    .requiredColumns("INPUT", IntStream.range(0, ((TableArgument) arguments.get("INPUT")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .build();
        }

        public static class TestInputProcessorProvider
                implements TableFunctionProcessorProvider
        {
            @Override
            public TableFunctionDataProcessor getDataProcessor(ConnectorTableFunctionHandle handle)
            {
                return new TestInputProcessor();
            }
        }

        private static class TestInputProcessor
                implements TableFunctionDataProcessor
        {
            private boolean processorGotInput;
            private boolean finished;

            @Override
            public TableFunctionProcessState process(List<Optional<Page>> input)
            {
                if (finished) {
                    return new TableFunctionProcessState(FINISHED, false, null, null);
                }
                if (input == null) {
                    finished = true;
                    BlockBuilder builder = BOOLEAN.createBlockBuilder(null, 1);
                    BOOLEAN.writeBoolean(builder, processorGotInput);
                    return new TableFunctionProcessState(FORWARD, false, new TableFunctionResult(new Page(builder.build())), null);
                }
                processorGotInput = true;
                return new TableFunctionProcessState(FORWARD, true, null, null);
            }
        }
    }

    public static class TestSingleInputRowSemanticsFunction
            extends AbstractConnectorTableFunction
    {
        public TestSingleInputRowSemanticsFunction()
        {
            super(
                    SCHEMA_NAME,
                    "test_single_input_function",
                    ImmutableList.of(TableArgumentSpecification.builder()
                            .rowSemantics()
                            .name("INPUT")
                            .build()),
                    new DescribedTable(new Descriptor(ImmutableList.of(new Descriptor.Field("boolean_result", Optional.of(BOOLEAN))))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new EmptyTableFunctionHandle())
                    .requiredColumns("INPUT", IntStream.range(0, ((TableArgument) arguments.get("INPUT")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .build();
        }

        public static class TestSingleInputFunctionProcessorProvider
                implements TableFunctionProcessorProvider
        {
            @Override
            public TableFunctionDataProcessor getDataProcessor(ConnectorTableFunctionHandle handle)
            {
                BlockBuilder builder = BOOLEAN.createBlockBuilder(null, 1);
                BOOLEAN.writeBoolean(builder, true);
                TableFunctionResult result = new TableFunctionResult(new Page(builder.build()));

                return input -> {
                    if (input == null) {
                        return new TableFunctionProcessState(FINISHED, false, null, null);
                    }
                    return new TableFunctionProcessState(FORWARD, true, result, null);
                };
            }
        }
    }

    public static class ConstantFunction
            extends AbstractConnectorTableFunction
    {
        public ConstantFunction()
        {
            super(
                    SCHEMA_NAME,
                    "constant",
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder()
                                    .name("VALUE")
                                    .type(INTEGER)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("N")
                                    .type(INTEGER)
                                    .defaultValue(1L)
                                    .build()),
                    new DescribedTable(Descriptor.descriptor(
                            ImmutableList.of("constant_column"),
                            ImmutableList.of(INTEGER))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            ScalarArgument count = (ScalarArgument) arguments.get("N");
            requireNonNull(count.getValue(), "count value for function repeat() is null");
            checkArgument((long) count.getValue() > 0, "count value for function repeat() must be positive");

            return TableFunctionAnalysis.builder()
                    .handle(new ConstantFunctionHandle(catalogHandle, (Long) ((ScalarArgument) arguments.get("VALUE")).getValue(), (long) count.getValue()))
                    .build();
        }

        public static class ConstantFunctionHandle
                implements ConnectorTableFunctionHandle
        {
            private final CatalogHandle catalogHandle;
            private final Long value;
            private final long count;

            @JsonCreator
            public ConstantFunctionHandle(@JsonProperty("catalogHandle") CatalogHandle catalogHandle, @JsonProperty("value") Long value, @JsonProperty("count") long count)
            {
                this.catalogHandle = catalogHandle;
                this.value = value;
                this.count = count;
            }

            @Override
            @JsonProperty
            public CatalogHandle getCatalogHandle()
            {
                return catalogHandle;
            }

            @JsonProperty
            public Long getValue()
            {
                return value;
            }

            @JsonProperty
            public long getCount()
            {
                return count;
            }
        }

        public static class ConstantFunctionProcessorProvider
                implements TableFunctionProcessorProvider
        {
            @Override
            public TableFunctionSplitProcessor getSplitProcessor(ConnectorTableFunctionHandle handle)
            {
                return new ConstantFunctionProcessor(((ConstantFunctionHandle) handle).getValue());
            }
        }

        public static class ConstantFunctionProcessor
                implements TableFunctionSplitProcessor
        {
            private static final int PAGE_SIZE = 1000;

            private final Long value;

            private long fullPagesCount;
            private long processedPages;
            private int reminder;
            private Block block;

            public ConstantFunctionProcessor(Long value)
            {
                this.value = value;
            }

            @Override
            public TableFunctionProcessState process(ConnectorSession session, ConnectorSplit split)
            {
                boolean usedData = false;

                if (split != null) {
                    long count = ((ConstantFunctionSplit) split).getCount();
                    this.fullPagesCount = count / PAGE_SIZE;
                    this.reminder = toIntExact(count % PAGE_SIZE);
                    if (fullPagesCount > 0) {
                        BlockBuilder builder = INTEGER.createBlockBuilder(null, PAGE_SIZE);
                        if (value == null) {
                            for (int i = 0; i < PAGE_SIZE; i++) {
                                builder.appendNull();
                            }
                        }
                        else {
                            for (int i = 0; i < PAGE_SIZE; i++) {
                                builder.writeInt(toIntExact(value));
                            }
                        }
                        this.block = builder.build();
                    }
                    else {
                        BlockBuilder builder = INTEGER.createBlockBuilder(null, reminder);
                        if (value == null) {
                            for (int i = 0; i < reminder; i++) {
                                builder.appendNull();
                            }
                        }
                        else {
                            for (int i = 0; i < reminder; i++) {
                                builder.writeInt(toIntExact(value));
                            }
                        }
                        this.block = builder.build();
                    }
                    usedData = true;
                }

                if (processedPages < fullPagesCount) {
                    processedPages++;
                    return new TableFunctionProcessState(FORWARD, usedData, new TableFunctionResult(new Page(block)), null);
                }

                if (reminder > 0) {
                    TableFunctionProcessState result = new TableFunctionProcessState(FORWARD, usedData, new TableFunctionResult(new Page(block.getRegion(0, toIntExact(reminder)))), null);
                    reminder = 0;
                    return result;
                }

                return new TableFunctionProcessState(FINISHED, false, null, null);
            }
        }

        public static ConnectorSplitSource getConstantFunctionSplitSource(ConstantFunctionHandle handle)
        {
            long splitSize = DEFAULT_SPLIT_SIZE;
            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
            for (long i = 0; i < handle.getCount() / splitSize; i++) {
                splits.add(new ConstantFunctionSplit(splitSize));
            }
            long remainingSize = handle.getCount() % splitSize;
            if (remainingSize > 0) {
                splits.add(new ConstantFunctionSplit(remainingSize));
            }
            return new FixedSplitSource(splits.build());
        }

        public static final class ConstantFunctionSplit
                implements ConnectorSplit
        {
            private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(ConstantFunctionSplit.class).instanceSize());
            public static final int DEFAULT_SPLIT_SIZE = 5500;

            private final long count;

            @JsonCreator
            public ConstantFunctionSplit(@JsonProperty("count") long count)
            {
                this.count = count;
            }

            @JsonProperty
            public long getCount()
            {
                return count;
            }

            @Override
            public boolean isRemotelyAccessible()
            {
                return true;
            }

            @Override
            public List<HostAddress> getAddresses()
            {
                return ImmutableList.of();
            }

            @Override
            public Object getInfo()
            {
                return count;
            }

            @Override
            public long getRetainedSizeInBytes()
            {
                return INSTANCE_SIZE;
            }
        }
    }

    public static class EmptyTableFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        @Override
        @JsonProperty
        public CatalogHandle getCatalogHandle()
        {
            return null;
        }
    }
}
