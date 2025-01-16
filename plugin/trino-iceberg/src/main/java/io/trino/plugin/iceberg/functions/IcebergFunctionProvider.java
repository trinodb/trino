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
import io.trino.plugin.base.classloader.ClassLoaderSafeTableFunctionProcessorProviderFactory;
import io.trino.plugin.iceberg.functions.tablechanges.TableChangesFunctionHandle;
import io.trino.plugin.iceberg.functions.tablechanges.TableChangesFunctionProcessorProviderFactory;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProviderFactory;

import static java.util.Objects.requireNonNull;

public class IcebergFunctionProvider
        implements FunctionProvider
{
    private final TableChangesFunctionProcessorProviderFactory tableChangesFunctionProcessorProviderFactory;

    @Inject
    public IcebergFunctionProvider(TableChangesFunctionProcessorProviderFactory tableChangesFunctionProcessorProviderFactory)
    {
        this.tableChangesFunctionProcessorProviderFactory = requireNonNull(tableChangesFunctionProcessorProviderFactory, "tableChangesFunctionProcessorProviderFactory is null");
    }

    @Override
    public TableFunctionProcessorProviderFactory getTableFunctionProcessorProviderFactory(ConnectorTableFunctionHandle functionHandle)
    {
        if (functionHandle instanceof TableChangesFunctionHandle) {
            return new ClassLoaderSafeTableFunctionProcessorProviderFactory(tableChangesFunctionProcessorProviderFactory, getClass().getClassLoader());
        }

        throw new UnsupportedOperationException("Unsupported function: " + functionHandle);
    }
}
