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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
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
import io.trino.spi.ptf.TableArgumentSpecification;
import io.trino.spi.ptf.TableFunctionAnalysis;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.ptf.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.ptf.ReturnTypeSpecification.OnlyPassThrough.ONLY_PASS_THROUGH;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestingTableFunctions
{
    private static final String SCHEMA_NAME = "system";
    private static final String TABLE_NAME = "table";
    private static final String COLUMN_NAME = "column";
    private static final ConnectorTableFunctionHandle HANDLE = new TestingTableFunctionHandle();
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
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            ScalarArgument argument = (ScalarArgument) arguments.get("COLUMN");
            String columnName = ((Slice) argument.getValue()).toStringUtf8();

            return TableFunctionAnalysis.builder()
                    .handle(new SimpleTableFunctionHandle(getSchema(), TABLE_NAME, columnName))
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field(columnName, Optional.of(BOOLEAN)))))
                    .build();
        }

        public static class SimpleTableFunctionHandle
                implements ConnectorTableFunctionHandle
        {
            private final MockConnectorTableHandle tableHandle;

            public SimpleTableFunctionHandle(String schema, String table, String column)
            {
                this.tableHandle = new MockConnectorTableHandle(
                        new SchemaTableName(schema, table),
                        TupleDomain.all(),
                        Optional.of(ImmutableList.of(new MockConnectorColumnHandle(column, BOOLEAN))));
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
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
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
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
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
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
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
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
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
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
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
                                    .keepWhenEmpty()
                                    .build()),
                    ONLY_PASS_THROUGH);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
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
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
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
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
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
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
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
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
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
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
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
        private final MockConnectorTableHandle tableHandle;

        public TestingTableFunctionHandle()
        {
            this.tableHandle = new MockConnectorTableHandle(
                    new SchemaTableName(SCHEMA_NAME, TABLE_NAME),
                    TupleDomain.all(),
                    Optional.of(ImmutableList.of(new MockConnectorColumnHandle(COLUMN_NAME, BOOLEAN))));
        }

        public MockConnectorTableHandle getTableHandle()
        {
            return tableHandle;
        }
    }
}
