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
package io.trino.plugin.lakehouse;

import com.google.inject.Inject;
import io.trino.plugin.deltalake.DeltaLakeFunctionProvider;
import io.trino.plugin.deltalake.functions.tablechanges.TableChangesTableFunctionHandle;
import io.trino.plugin.iceberg.functions.IcebergFunctionProvider;
import io.trino.plugin.iceberg.functions.tablechanges.TableChangesFunctionHandle;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProviderFactory;

import static java.util.Objects.requireNonNull;

public class LakehouseFunctionProvider
        implements FunctionProvider
{
    private final DeltaLakeFunctionProvider deltaLakeFunctionProvider;
    private final IcebergFunctionProvider icebergFunctionProvider;

    @Inject
    public LakehouseFunctionProvider(
            DeltaLakeFunctionProvider deltaLakeFunctionProvider,
            IcebergFunctionProvider icebergFunctionProvider)
    {
        this.deltaLakeFunctionProvider = requireNonNull(deltaLakeFunctionProvider, "deltaLakeFunctionProvider is null");
        this.icebergFunctionProvider = requireNonNull(icebergFunctionProvider, "icebergFunctionProvider is null");
    }

    @Override
    public TableFunctionProcessorProviderFactory getTableFunctionProcessorProviderFactory(ConnectorTableFunctionHandle functionHandle)
    {
        if (functionHandle instanceof TableChangesTableFunctionHandle) {
            return deltaLakeFunctionProvider.getTableFunctionProcessorProviderFactory(functionHandle);
        }
        if (functionHandle instanceof TableChangesFunctionHandle) {
            return icebergFunctionProvider.getTableFunctionProcessorProviderFactory(functionHandle);
        }
        throw new UnsupportedOperationException("Unsupported function: " + functionHandle);
    }
}
