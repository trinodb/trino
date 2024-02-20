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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

/**
 * This factory is used to pass spatial index built by SpatialIndexBuilderOperator
 * to SpatialJoinOperators.
 * <p>
 * SpatialIndexBuilderOperator creates a spatial index {@link Supplier} and provides
 * it to this factory by calling {@link #lendPagesSpatialIndex(Supplier)}.
 * <p>
 * SpatialJoinOperators call {@link #createPagesSpatialIndex()} to get a Future that
 * will provide an instance of the spatial index when done. The {@link Supplier}
 * is used to create separate instances of an index for each SpatialJoinOperator. All
 * these instances share the index of geometries on the build side, but have their own
 * instances of an optional extra potentially stateful filter function.
 * <p>
 * SpatialIndexBuilderOperator is responsible for keeping track of how much memory
 * is used by the index shared among SpatialJoinOperators. To do so
 * SpatialIndexBuilderOperator has to stay active until all the SpatialJoinOperators
 * have finished.
 * <p>
 * {@link #lendPagesSpatialIndex(Supplier)} returns a Future that completes once all
 * the SpatialJoinOperators completed. SpatialIndexBuilderOperator uses that Future
 * to decide on its own completion.
 */
@ThreadSafe
public class PagesSpatialIndexFactory
{
    private final List<Type> types;
    private final List<Type> outputTypes;

    @GuardedBy("this")
    private final List<SettableFuture<PagesSpatialIndex>> pagesSpatialIndexFutures = new ArrayList<>();

    @GuardedBy("this")
    @Nullable
    private Supplier<PagesSpatialIndex> pagesSpatialIndex;

    private final SettableFuture<Void> destroyed = SettableFuture.create();

    public PagesSpatialIndexFactory(List<Type> types, List<Type> outputTypes)
    {
        this.types = ImmutableList.copyOf(types);
        this.outputTypes = ImmutableList.copyOf(outputTypes);
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    public void destroy()
    {
        destroyed.set(null);
        synchronized (this) {
            pagesSpatialIndex = null;
            pagesSpatialIndexFutures.clear();
        }
    }

    /**
     * Called by {@link SpatialJoinOperator}.
     */
    public synchronized ListenableFuture<PagesSpatialIndex> createPagesSpatialIndex()
    {
        checkState(!destroyed.isDone(), "already destroyed");
        if (pagesSpatialIndex != null) {
            return immediateFuture(pagesSpatialIndex.get());
        }

        SettableFuture<PagesSpatialIndex> future = SettableFuture.create();
        pagesSpatialIndexFutures.add(future);
        return future;
    }

    /**
     * Called by {@link SpatialIndexBuilderOperator} to provide a
     * {@link Supplier} of spatial indexes for {@link SpatialJoinOperator}s to use.
     * <p>
     * Returns a Future that completes once all the {@link SpatialJoinOperator}s have completed.
     */
    public ListenableFuture<Void> lendPagesSpatialIndex(Supplier<PagesSpatialIndex> pagesSpatialIndex)
    {
        requireNonNull(pagesSpatialIndex, "pagesSpatialIndex is null");

        List<SettableFuture<PagesSpatialIndex>> settableFutures;
        synchronized (this) {
            if (destroyed.isDone()) {
                return destroyed;
            }

            verify(this.pagesSpatialIndex == null);
            this.pagesSpatialIndex = pagesSpatialIndex;
            settableFutures = ImmutableList.copyOf(pagesSpatialIndexFutures);
            pagesSpatialIndexFutures.clear();
        }

        for (SettableFuture<PagesSpatialIndex> settableFuture : settableFutures) {
            settableFuture.set(pagesSpatialIndex.get());
        }

        return destroyed;
    }
}
