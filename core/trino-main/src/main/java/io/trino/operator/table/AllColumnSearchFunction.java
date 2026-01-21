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
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableArgument;
import io.trino.spi.function.table.TableArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.type.CharType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metadata.GlobalFunctionCatalog.BUILTIN_SCHEMA;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.usedInputAndProduced;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class AllColumnSearchFunction
        extends AbstractConnectorTableFunction
{
    public static final String NAME = "allcolumnsearch";

    private static final String TABLE_ARGUMENT_NAME = "INPUT";
    private static final String SEARCH_TERM_ARGUMENT_NAME = "SEARCH_TERM";

    public AllColumnSearchFunction()
    {
        super(
                BUILTIN_SCHEMA,
                NAME,
                ImmutableList.of(
                        TableArgumentSpecification.builder()
                                .name(TABLE_ARGUMENT_NAME)
                                .rowSemantics()
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(SEARCH_TERM_ARGUMENT_NAME)
                                .type(VARCHAR)
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
        // Get search term argument
        ScalarArgument searchTermArgument = (ScalarArgument) arguments.get(SEARCH_TERM_ARGUMENT_NAME);
        if (searchTermArgument.getValue() == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Search term cannot be null");
        }

        String searchTerm = ((Slice) searchTermArgument.getValue()).toStringUtf8();
        if (searchTerm.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Search term cannot be empty");
        }

        // Validate regex pattern
        try {
            Pattern.compile(searchTerm);
        }
        catch (PatternSyntaxException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid regex pattern: " + e.getMessage());
        }

        // Get input table schema
        TableArgument tableArgument = (TableArgument) arguments.get(TABLE_ARGUMENT_NAME);
        RowType inputRowType = tableArgument.getRowType();
        List<RowType.Field> inputFields = inputRowType.getFields();

        // Find all string columns (VARCHAR/CHAR types)
        ImmutableList.Builder<Integer> stringColumnIndices = ImmutableList.builder();
        for (int i = 0; i < inputFields.size(); i++) {
            Type type = inputFields.get(i).getType();
            if (type instanceof VarcharType || type instanceof CharType) {
                stringColumnIndices.add(i);
            }
        }

        List<Integer> searchableColumns = stringColumnIndices.build();
        if (searchableColumns.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "No searchable string columns found in input table");
        }

        // Request all columns from input
        ImmutableList.Builder<Integer> requiredColumns = ImmutableList.builder();
        for (int i = 0; i < inputFields.size(); i++) {
            requiredColumns.add(i);
        }

        // Return type is same as input type
        List<Descriptor.Field> returnedType = inputFields.stream()
                .map(field -> new Descriptor.Field(field.getName(), Optional.of(field.getType())))
                .collect(toImmutableList());

        return TableFunctionAnalysis.builder()
                .requiredColumns(TABLE_ARGUMENT_NAME, requiredColumns.build())
                .returnedType(new Descriptor(returnedType))
                .handle(new AllColumnSearchFunctionHandle(searchTerm, searchableColumns))
                .build();
    }

    public static TableFunctionProcessorProvider getAllColumnSearchFunctionProcessorProvider()
    {
        return new TableFunctionProcessorProvider()
        {
            @Override
            public TableFunctionDataProcessor getDataProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle)
            {
                AllColumnSearchFunctionHandle functionHandle = (AllColumnSearchFunctionHandle) handle;
                String searchTerm = functionHandle.searchTerm();
                List<Integer> searchableColumns = functionHandle.searchableColumnIndices();

                // Compile regex pattern once for reuse
                Pattern searchPattern = Pattern.compile(searchTerm);

                return input -> {
                    if (input == null) {
                        return FINISHED;
                    }

                    Page inputPage = getOnlyElement(input).orElseThrow();

                    // For simplicity, just filter the page directly by checking each row
                    int[] selectedPositions = new int[inputPage.getPositionCount()];
                    int selectedCount = 0;

                    for (int position = 0; position < inputPage.getPositionCount(); position++) {
                        boolean matches = false;

                        // Check each searchable column
                        for (int columnIndex : searchableColumns) {
                            Block block = inputPage.getBlock(columnIndex);
                            if (!block.isNull(position)) {
                                Slice value = (Slice) VARCHAR.getObject(block, position);
                                String columnValue = value.toStringUtf8();

                                if (searchPattern.matcher(columnValue).find()) {
                                    matches = true;
                                    break;
                                }
                            }
                        }

                        if (matches) {
                            selectedPositions[selectedCount++] = position;
                        }
                    }

                    // Return filtered page with only matching rows
                    if (selectedCount == 0) {
                        // No matches, return empty page
                        return usedInputAndProduced(inputPage.getPositions(new int[0], 0, 0));
                    }
                    else if (selectedCount == inputPage.getPositionCount()) {
                        // All rows match, return original page
                        return usedInputAndProduced(inputPage);
                    }
                    else {
                        // Some rows match, return filtered page
                        return usedInputAndProduced(inputPage.getPositions(selectedPositions, 0, selectedCount));
                    }
                };
            }
        };
    }

    public record AllColumnSearchFunctionHandle(String searchTerm, List<Integer> searchableColumnIndices)
            implements ConnectorTableFunctionHandle
    {
    }
}
