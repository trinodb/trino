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
package io.trino.operator.index;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.trino.operator.PagesIndex;
import io.trino.operator.TaskContext;
import io.trino.operator.join.LookupSourceFactory;
import io.trino.operator.join.LookupSourceProvider;
import io.trino.operator.join.OuterPositionIterator;
import io.trino.operator.join.StaticLookupSourceProvider;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class IndexLookupSourceFactory
        implements LookupSourceFactory
{
    private final List<Type> outputTypes;
    private final Supplier<IndexLoader> indexLoaderSupplier;
    private TaskContext taskContext;
    private final SettableFuture<Void> whenTaskContextSet = SettableFuture.create();

    public IndexLookupSourceFactory(
            Set<Integer> lookupSourceInputChannels,
            List<Integer> keyOutputChannels,
            OptionalInt keyOutputHashChannel,
            List<Type> outputTypes,
            IndexBuildDriverFactoryProvider indexBuildDriverFactoryProvider,
            DataSize maxIndexMemorySize,
            IndexJoinLookupStats stats,
            boolean shareIndexLoading,
            PagesIndex.Factory pagesIndexFactory,
            JoinCompiler joinCompiler,
            BlockTypeOperators blockTypeOperators)
    {
        this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));

        if (shareIndexLoading) {
            IndexLoader shared = new IndexLoader(lookupSourceInputChannels, keyOutputChannels, keyOutputHashChannel, outputTypes, indexBuildDriverFactoryProvider, 10_000, maxIndexMemorySize, stats, pagesIndexFactory, joinCompiler, blockTypeOperators);
            this.indexLoaderSupplier = () -> shared;
        }
        else {
            this.indexLoaderSupplier = () -> new IndexLoader(lookupSourceInputChannels, keyOutputChannels, keyOutputHashChannel, outputTypes, indexBuildDriverFactoryProvider, 10_000, maxIndexMemorySize, stats, pagesIndexFactory, joinCompiler, blockTypeOperators);
        }
    }

    @Override
    public List<Type> getTypes()
    {
        return outputTypes;
    }

    @Override
    public List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    @Override
    public void setTaskContext(TaskContext taskContext)
    {
        this.taskContext = taskContext;
        whenTaskContextSet.set(null);
    }

    @Override
    public ListenableFuture<LookupSourceProvider> createLookupSourceProvider()
    {
        checkState(taskContext != null, "taskContext not set");

        IndexLoader indexLoader = indexLoaderSupplier.get();
        indexLoader.setContext(taskContext);
        return Futures.immediateFuture(new StaticLookupSourceProvider(new IndexLookupSource(indexLoader)));
    }

    @Override
    public ListenableFuture<Void> whenBuildFinishes()
    {
        return Futures.transformAsync(
                whenTaskContextSet,
                ignored -> transform(
                        this.createLookupSourceProvider(),
                        lookupSourceProvider -> {
                            // Close the lookupSourceProvider we just created.
                            // The only reason we created it is to wait until lookup source is ready.
                            lookupSourceProvider.close();
                            return null;
                        },
                        directExecutor()),
                directExecutor());
    }

    @Override
    public int partitions()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public OuterPositionIterator getOuterPositionIterator()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void destroy()
    {
        // nothing to do
    }
}
