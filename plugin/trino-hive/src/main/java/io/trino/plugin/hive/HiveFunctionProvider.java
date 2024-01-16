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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.plugin.base.classloader.ClassLoaderSafeTableFunctionProcessorProvider;
import io.trino.plugin.hive.functions.Unload.UnloadFunctionHandle;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;

import java.util.Set;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.hive.functions.Unload.getUnloadFunctionProcessorProvider;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class HiveFunctionProvider
        implements FunctionProvider
{
    private final Set<HiveFileWriterFactory> fileWriterFactories;
    private final HiveWriterStats hiveWriterStats;
    private final PageIndexerFactory pageIndexerFactory;
    private final ListeningExecutorService writeVerificationExecutor;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;

    @Inject
    public HiveFunctionProvider(
            Set<HiveFileWriterFactory> fileWriterFactories,
            HiveWriterStats hiveWriterStats,
            PageIndexerFactory pageIndexerFactory,
            HiveConfig config,
            JsonCodec<PartitionUpdate> partitionUpdateCodec)
    {
        this.fileWriterFactories = ImmutableSet.copyOf(requireNonNull(fileWriterFactories, "fileWriterFactories is null"));
        this.hiveWriterStats = requireNonNull(hiveWriterStats, "hiveWriterStats is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.writeVerificationExecutor = listeningDecorator(newFixedThreadPool(config.getWriteValidationThreads(), daemonThreadsNamed("hive-write-validation-%s")));
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
    }

    @Override
    public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle functionHandle)
    {
        if (functionHandle instanceof UnloadFunctionHandle) {
            return new ClassLoaderSafeTableFunctionProcessorProvider(getUnloadFunctionProcessorProvider(
                    fileWriterFactories,
                    hiveWriterStats,
                    pageIndexerFactory,
                    writeVerificationExecutor,
                    partitionUpdateCodec),
                    getClass().getClassLoader());
        }
        throw new UnsupportedOperationException("Unsupported function: " + functionHandle);
    }
}
