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
package io.trino.operator.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.inject.Provider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.DescriptorArgument;
import io.trino.spi.function.table.DescriptorArgumentSpecification;
import io.trino.spi.function.table.TableArgument;
import io.trino.spi.function.table.TableArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.type.RowType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metadata.GlobalFunctionCatalog.BUILTIN_SCHEMA;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.DescriptorArgument.NULL_DESCRIPTOR;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.usedInputAndProduced;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

public class ExcludeColumns
        implements Provider<ConnectorTableFunction>
{
    public static final String NAME = "exclude_columns";

    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(new ExcludeColumnsFunction(), getClass().getClassLoader());
    }

    public static class ExcludeColumnsFunction
            extends AbstractConnectorTableFunction
    {
        private static final String TABLE_ARGUMENT_NAME = "INPUT";
        private static final String DESCRIPTOR_ARGUMENT_NAME = "COLUMNS";

        public ExcludeColumnsFunction()
        {
            super(
                    BUILTIN_SCHEMA,
                    NAME,
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name(TABLE_ARGUMENT_NAME)
                                    .rowSemantics()
                                    .build(),
                            DescriptorArgumentSpecification.builder()
                                    .name(DESCRIPTOR_ARGUMENT_NAME)
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl)
        {
            DescriptorArgument excludedColumns = (DescriptorArgument) arguments.get(DESCRIPTOR_ARGUMENT_NAME);
            if (excludedColumns.equals(NULL_DESCRIPTOR)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "COLUMNS descriptor is null");
            }
            Descriptor excludedColumnsDescriptor = excludedColumns.getDescriptor().orElseThrow();
            if (excludedColumnsDescriptor.getFields().stream().anyMatch(field -> field.getType().isPresent())) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "COLUMNS descriptor contains types");
            }

            // column names in DescriptorArgument are canonical wrt SQL identifier semantics.
            // column names in TableArgument are not canonical wrt SQL identifier semantics, as they are taken from the corresponding RelationType.
            // because of that, we match the excluded columns names case-insensitive
            // TODO apply proper identifier semantics
            Set<String> excludedNames = excludedColumnsDescriptor.getFields().stream()
                    .map(Descriptor.Field::getName)
                    .map(name -> name.orElseThrow().toLowerCase(ENGLISH))
                    .collect(toImmutableSet());

            List<RowType.Field> inputSchema = ((TableArgument) arguments.get(TABLE_ARGUMENT_NAME)).getRowType().getFields();
            Set<String> inputNames = inputSchema.stream()
                    .map(RowType.Field::getName)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(name -> name.toLowerCase(ENGLISH))
                    .collect(toImmutableSet());

            if (!inputNames.containsAll(excludedNames)) {
                String missingColumns = Sets.difference(excludedNames, inputNames).stream()
                        .collect(joining(", ", "[", "]"));
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Excluded columns: %s not present in the table", missingColumns));
            }

            ImmutableList.Builder<Integer> requiredColumns = ImmutableList.builder();
            ImmutableList.Builder<Descriptor.Field> returnedColumns = ImmutableList.builder();

            for (int i = 0; i < inputSchema.size(); i++) {
                Optional<String> name = inputSchema.get(i).getName();
                if (name.isEmpty() || !excludedNames.contains(name.orElseThrow().toLowerCase(ENGLISH))) {
                    requiredColumns.add(i);
                    // per SQL standard, all columns produced by a table function must be named. We allow anonymous columns.
                    returnedColumns.add(new Descriptor.Field(name, Optional.of(inputSchema.get(i).getType())));
                }
            }

            List<Descriptor.Field> returnedType = returnedColumns.build();
            if (returnedType.isEmpty()) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "All columns are excluded");
            }

            return TableFunctionAnalysis.builder()
                    .requiredColumns(TABLE_ARGUMENT_NAME, requiredColumns.build())
                    .returnedType(new Descriptor(returnedType))
                    .handle(new ExcludeColumnsFunctionHandle())
                    .build();
        }
    }

    public static TableFunctionProcessorProvider getExcludeColumnsFunctionProcessorProvider()
    {
        return new TableFunctionProcessorProvider()
        {
            @Override
            public TableFunctionDataProcessor getDataProcessor(ConnectorTableFunctionHandle handle)
            {
                return input -> {
                    if (input == null) {
                        return FINISHED;
                    }
                    return usedInputAndProduced(getOnlyElement(input).orElseThrow());
                };
            }
        };
    }

    public record ExcludeColumnsFunctionHandle()
            implements ConnectorTableFunctionHandle
    {
        // there's no information to remember. All logic is effectively delegated to the engine via `requiredColumns`.
    }
}
