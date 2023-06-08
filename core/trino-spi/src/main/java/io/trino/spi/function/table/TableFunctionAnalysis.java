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
package io.trino.spi.function.table;

import io.trino.spi.Experimental;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.function.table.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * An object of this class is produced by the `analyze()` method of a `ConnectorTableFunction`
 * implementation. It contains all the analysis results:
 * <p>
 * The `returnedType` field is used to inform the Analyzer of the proper columns returned by the Table
 * Function, that is, the columns produced by the function, as opposed to the columns passed from the
 * input tables. The `returnedType` should only be set if the declared returned type is GENERIC_TABLE.
 * <p>
 * The `requiredColumns` field is used to inform the Analyzer of the columns from the table arguments
 * that are necessary to execute the table function.
 * <p>
 * The `handle` field can be used to carry all information necessary to execute the table function,
 * gathered at analysis time. Typically, these are the values of the constant arguments, and results
 * of pre-processing arguments.
 */
@Experimental(eta = "2022-10-31")
public final class TableFunctionAnalysis
{
    private final Optional<Descriptor> returnedType;

    // a map from table argument name to list of column indexes for all columns required from the table argument
    private final Map<String, List<Integer>> requiredColumns;
    private final ConnectorTableFunctionHandle handle;

    private TableFunctionAnalysis(Optional<Descriptor> returnedType, Map<String, List<Integer>> requiredColumns, ConnectorTableFunctionHandle handle)
    {
        this.returnedType = requireNonNull(returnedType, "returnedType is null");
        returnedType.ifPresent(descriptor -> checkArgument(descriptor.isTyped(), "field types not specified"));
        this.requiredColumns = Map.copyOf(requiredColumns.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> List.copyOf(entry.getValue()))));
        this.handle = requireNonNull(handle, "handle is null");
    }

    public Optional<Descriptor> getReturnedType()
    {
        return returnedType;
    }

    public Map<String, List<Integer>> getRequiredColumns()
    {
        return requiredColumns;
    }

    public ConnectorTableFunctionHandle getHandle()
    {
        return handle;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Descriptor returnedType;
        private final Map<String, List<Integer>> requiredColumns = new HashMap<>();
        private ConnectorTableFunctionHandle handle;

        private Builder() {}

        public Builder returnedType(Descriptor returnedType)
        {
            this.returnedType = returnedType;
            return this;
        }

        public Builder requiredColumns(String tableArgument, List<Integer> columns)
        {
            this.requiredColumns.put(tableArgument, columns);
            return this;
        }

        public Builder handle(ConnectorTableFunctionHandle handle)
        {
            this.handle = handle;
            return this;
        }

        public TableFunctionAnalysis build()
        {
            return new TableFunctionAnalysis(Optional.ofNullable(returnedType), requiredColumns, handle);
        }
    }
}
