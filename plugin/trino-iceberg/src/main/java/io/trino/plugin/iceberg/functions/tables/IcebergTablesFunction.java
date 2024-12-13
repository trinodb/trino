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
package io.trino.plugin.iceberg.functions.tables;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.Page;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.produced;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class IcebergTablesFunction
        extends AbstractConnectorTableFunction
{
    private static final String FUNCTION_NAME = "iceberg_tables";
    private static final String SCHEMA_NAME_VAR_NAME = "SCHEMA_NAME";

    private final TrinoCatalogFactory trinoCatalogFactory;

    public IcebergTablesFunction(TrinoCatalogFactory trinoCatalogFactory)
    {
        super(
                "system",
                FUNCTION_NAME,
                ImmutableList.of(
                        ScalarArgumentSpecification.builder()
                                .name(SCHEMA_NAME_VAR_NAME)
                                .type(VARCHAR)
                                .defaultValue(null)
                                .build()),
                GENERIC_TABLE);
        this.trinoCatalogFactory = requireNonNull(trinoCatalogFactory, "trinoCatalogFactory is null");
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl)
    {
        ScalarArgument argument = (ScalarArgument) getOnlyElement(arguments.values());
        Optional<String> schemaFilter = Optional.ofNullable(((Slice) argument.getValue())).map(Slice::toStringUtf8);

        TrinoCatalog catalog = trinoCatalogFactory.create(session.getIdentity());
        List<SchemaTableName> tables = catalog.listIcebergTables(session, schemaFilter);
        Set<SchemaTableName> filtered = accessControl.filterTables(null, ImmutableSet.copyOf(tables));
        return TableFunctionAnalysis.builder()
                .returnedType(new Descriptor(ImmutableList.of(
                        new Descriptor.Field("table_schema", Optional.of(VARCHAR)),
                        new Descriptor.Field("table_name", Optional.of(VARCHAR)))))
                .handle(new IcebergTables(filtered))
                .build();
    }

    public record IcebergTables(Collection<SchemaTableName> tables)
            implements ConnectorTableFunctionHandle, ConnectorSplit
    {
        public IcebergTables
        {
            requireNonNull(tables, "tables is null");
        }
    }

    public static class IcebergTablesProcessor
            implements TableFunctionSplitProcessor
    {
        private final Collection<SchemaTableName> tables;
        private boolean finished;

        public IcebergTablesProcessor(Collection<SchemaTableName> tables)
        {
            this.tables = requireNonNull(tables, "tables is null");
        }

        @Override
        public TableFunctionProcessorState process()
        {
            if (finished) {
                return FINISHED;
            }

            VariableWidthBlockBuilder schema = VARCHAR.createBlockBuilder(null, tables.size());
            VariableWidthBlockBuilder tableName = VARCHAR.createBlockBuilder(null, tables.size());
            for (SchemaTableName table : tables) {
                schema.writeEntry(Slices.utf8Slice(table.getSchemaName()));
                tableName.writeEntry(Slices.utf8Slice(table.getTableName()));
            }
            finished = true;
            return produced(new Page(tables.size(), schema.build(), tableName.build()));
        }
    }
}
