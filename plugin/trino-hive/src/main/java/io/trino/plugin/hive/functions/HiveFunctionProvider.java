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
package io.trino.plugin.hive.functions;

import com.google.inject.Inject;
import io.trino.plugin.hive.HiveFileWriterFactory;
import io.trino.plugin.hive.functions.unload.UnloadFunctionDataProcessor;
import io.trino.plugin.hive.functions.unload.UnloadFunctionHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionProcessorProviderFactory;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class HiveFunctionProvider
        implements FunctionProvider
{
    private final Set<HiveFileWriterFactory> fileWriterFactories;

    @Inject
    public HiveFunctionProvider(Set<HiveFileWriterFactory> fileWriterFactories)
    {
        this.fileWriterFactories = requireNonNull(fileWriterFactories, "fileWriterFactories is null");
    }

    @Override
    public TableFunctionProcessorProviderFactory getTableFunctionProcessorProviderFactory(ConnectorTableFunctionHandle functionHandle)
    {
        if (functionHandle instanceof UnloadFunctionHandle unloadFunctionHandle) {
            return () -> new UnloadProcessorProvider(fileWriterFactories, unloadFunctionHandle);
        }
        throw new UnsupportedOperationException("Unsupported function: " + functionHandle);
    }

    private record UnloadProcessorProvider(Set<HiveFileWriterFactory> fileWriterFactories, UnloadFunctionHandle handle)
            implements TableFunctionProcessorProvider
    {
        @Override
        public TableFunctionDataProcessor getDataProcessor(
                ConnectorSession session,
                ConnectorTableFunctionHandle functionHandle)
        {
            return new UnloadFunctionDataProcessor(session, fileWriterFactories, (UnloadFunctionHandle) functionHandle);
        }
    }
}
