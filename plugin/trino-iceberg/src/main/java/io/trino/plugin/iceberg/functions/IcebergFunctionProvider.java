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
package io.trino.plugin.iceberg.functions;

import com.google.inject.Inject;
import io.trino.plugin.base.classloader.ClassLoaderSafeTableFunctionProcessorProvider;
import io.trino.plugin.iceberg.functions.tablechanges.TableChangesFunctionHandle;
import io.trino.plugin.iceberg.functions.tablechanges.TableChangesFunctionProcessorProvider;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;

import static java.util.Objects.requireNonNull;

public class IcebergFunctionProvider
        implements FunctionProvider
{
    private final TableChangesFunctionProcessorProvider tableChangesFunctionProcessorProvider;

    @Inject
    public IcebergFunctionProvider(TableChangesFunctionProcessorProvider tableChangesFunctionProcessorProvider)
    {
        this.tableChangesFunctionProcessorProvider = requireNonNull(tableChangesFunctionProcessorProvider, "tableChangesFunctionProcessorProvider is null");
    }

    @Override
    public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle functionHandle)
    {
        if (functionHandle instanceof TableChangesFunctionHandle) {
            return new ClassLoaderSafeTableFunctionProcessorProvider(tableChangesFunctionProcessorProvider, getClass().getClassLoader());
        }

        throw new UnsupportedOperationException("Unsupported function: " + functionHandle);
    }
}
