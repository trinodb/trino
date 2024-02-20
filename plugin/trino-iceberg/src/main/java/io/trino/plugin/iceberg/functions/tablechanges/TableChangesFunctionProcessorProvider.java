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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

import static java.util.Objects.requireNonNull;

public class TableChangesFunctionProcessorProvider
        implements TableFunctionProcessorProvider
{
    private final IcebergPageSourceProvider icebergPageSourceProvider;

    @Inject
    public TableChangesFunctionProcessorProvider(IcebergPageSourceProvider icebergPageSourceProvider)
    {
        this.icebergPageSourceProvider = requireNonNull(icebergPageSourceProvider, "icebergPageSourceProvider is null");
    }

    @Override
    public TableFunctionSplitProcessor getSplitProcessor(
            ConnectorSession session,
            ConnectorTableFunctionHandle handle,
            ConnectorSplit split)
    {
        return new ClassLoaderSafeTableFunctionSplitProcessor(new TableChangesFunctionProcessor(
                session,
                (TableChangesFunctionHandle) handle,
                (TableChangesSplit) split,
                icebergPageSourceProvider),
                getClass().getClassLoader());
    }
}
