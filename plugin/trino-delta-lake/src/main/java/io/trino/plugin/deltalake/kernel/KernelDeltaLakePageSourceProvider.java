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
package io.trino.plugin.deltalake.kernel;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.TypeManager;

import java.util.List;

public class KernelDeltaLakePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final TrinoFileSystemFactory trinoFileSystemFactory;
    private final TypeManager typeManager;

    @Inject
    public KernelDeltaLakePageSourceProvider(
            TrinoFileSystemFactory trinoFileSystemFactory,
            TypeManager typeManager)
    {
        this.trinoFileSystemFactory = trinoFileSystemFactory;
        this.typeManager = typeManager;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        KernelDeltaLakeSplit kernelDeltaLakeSplit = (KernelDeltaLakeSplit) split;
        return new KernelDeltaLakePageSource(
                trinoFileSystemFactory.create(session),
                typeManager,
                kernelDeltaLakeSplit,
                (KernelDeltaLakeTableHandle) table);
    }
}
