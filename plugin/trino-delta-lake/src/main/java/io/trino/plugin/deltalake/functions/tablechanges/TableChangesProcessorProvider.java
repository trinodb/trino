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
package io.trino.plugin.deltalake.functions.tablechanges;

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.spi.ptf.ConnectorTableFunctionHandle;
import io.trino.spi.ptf.TableFunctionProcessorProvider;
import io.trino.spi.ptf.TableFunctionSplitProcessor;

public class TableChangesProcessorProvider
        implements TableFunctionProcessorProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final DeltaLakeConfig deltaLakeConfig;

    public TableChangesProcessorProvider(TrinoFileSystemFactory fileSystemFactory, DeltaLakeConfig deltaLakeConfig)
    {
        this.fileSystemFactory = fileSystemFactory;
        this.deltaLakeConfig = deltaLakeConfig;
    }

    @Override
    public TableFunctionSplitProcessor getSplitProcessor(ConnectorTableFunctionHandle handle)
    {
        return new TableChangesFunctionProcessor(fileSystemFactory, deltaLakeConfig, (TableChangesFunctionTableHandle) handle);
    }
}
