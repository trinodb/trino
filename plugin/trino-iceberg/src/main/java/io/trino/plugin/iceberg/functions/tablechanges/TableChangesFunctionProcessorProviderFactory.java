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
package io.trino.plugin.iceberg.functions.tablechanges;

import com.google.inject.Inject;
import io.trino.plugin.base.classloader.ClassLoaderSafeTableFunctionSplitProcessor;
import io.trino.plugin.iceberg.IcebergPageSourceProvider;
import io.trino.plugin.iceberg.IcebergPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionProcessorProviderFactory;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.spi.type.TypeManager;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TableChangesFunctionProcessorProviderFactory
        implements TableFunctionProcessorProviderFactory
{
    private final IcebergPageSourceProviderFactory icebergPageSourceProviderFactory;
    private final TypeManager typeManager;

    @Inject
    public TableChangesFunctionProcessorProviderFactory(IcebergPageSourceProviderFactory icebergPageSourceProviderFactory, TypeManager typeManager)
    {
        this.icebergPageSourceProviderFactory = requireNonNull(icebergPageSourceProviderFactory, "icebergPageSourceProviderFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public TableFunctionProcessorProvider createTableFunctionProcessorProvider()
    {
        IcebergPageSourceProvider pageSourceProvider = (IcebergPageSourceProvider) icebergPageSourceProviderFactory.createPageSourceProvider(MemoryContext.NO_LIMIT);
        return new TableChangesFunctionProcessorProvider(pageSourceProvider, typeManager);
    }

    private record TableChangesFunctionProcessorProvider(IcebergPageSourceProvider icebergPageSourceProvider, TypeManager typeManager)
            implements TableFunctionProcessorProvider
    {
        @Override
        public TableFunctionSplitProcessor getSplitProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle, Optional<ConnectorTableCredentials> tableCredentials, ConnectorSplit split)
        {
            return new ClassLoaderSafeTableFunctionSplitProcessor(
                    new TableChangesFunctionProcessor(
                            session,
                            (TableChangesFunctionHandle) handle,
                            tableCredentials,
                            (TableChangesSplit) split,
                            icebergPageSourceProvider,
                            typeManager),
                    getClass().getClassLoader());
        }
    }
}
