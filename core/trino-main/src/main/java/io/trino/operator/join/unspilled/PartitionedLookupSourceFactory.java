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
package io.trino.operator.join.unspilled;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.operator.join.JoinBridge;
import io.trino.operator.join.LookupSource;
import io.trino.operator.join.OuterPositionIterator;
import io.trino.operator.join.TrackingLookupSourceSupplier;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.operator.join.OuterLookupSource.createOuterLookupSourceSupplier;
import static io.trino.operator.join.unspilled.PartitionedLookupSource.createPartitionedLookupSourceSupplier;
import static java.util.Objects.requireNonNull;

public final class PartitionedLookupSourceFactory
        implements JoinBridge
{
    private final List<Type> types;
    private final List<Type> outputTypes;
    private final List<Type> hashChannelTypes;
    private final boolean outer;
    private final BlockTypeOperators blockTypeOperators;

    @GuardedBy("this")
    private final Supplier<LookupSource>[] partitions;

    private final SettableFuture<Void> partitionsNoLongerNeeded = SettableFuture.create();

    @GuardedBy("this")
    private final SettableFuture<Void> destroyed = SettableFuture.create();

    @GuardedBy("this")
    private int partitionsSet;

    @GuardedBy("this")
    private TrackingLookupSourceSupplier lookupSourceSupplier;

    @GuardedBy("this")
    private final List<SettableFuture<LookupSource>> lookupSourceFutures = new ArrayList<>();

    public PartitionedLookupSourceFactory(List<Type> types, List<Type> outputTypes, List<Type> hashChannelTypes, int partitionCount, boolean outer, BlockTypeOperators blockTypeOperators)
    {
        checkArgument(Integer.bitCount(partitionCount) == 1, "partitionCount must be a power of 2");

        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
        this.hashChannelTypes = ImmutableList.copyOf(hashChannelTypes);
        checkArgument(partitionCount > 0);
        //noinspection unchecked
        this.partitions = (Supplier<LookupSource>[]) new Supplier<?>[partitionCount];
        this.outer = outer;
        this.blockTypeOperators = blockTypeOperators;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    // partitions is final, so we don't need a lock to read its length here
    @SuppressWarnings("FieldAccessNotGuarded")
    public int partitions()
    {
        return partitions.length;
    }

    public synchronized ListenableFuture<LookupSource> createLookupSource()
    {
        checkState(!destroyed.isDone(), "already destroyed");
        if (lookupSourceSupplier != null) {
            return immediateFuture(lookupSourceSupplier.getLookupSource());
        }

        SettableFuture<LookupSource> lookupSourceFuture = SettableFuture.create();
        lookupSourceFutures.add(lookupSourceFuture);
        return lookupSourceFuture;
    }

    @Override
    public ListenableFuture<Void> whenBuildFinishes()
    {
        return transform(
                this.createLookupSource(),
                lookupSourceProvider -> {
                    // Close the lookupSourceProvider we just created.
                    // The only reason we created it is to wait until lookup source is ready.
                    lookupSourceProvider.close();
                    return null;
                },
                directExecutor());
    }

    public ListenableFuture<Void> lendPartitionLookupSource(int partitionIndex, Supplier<LookupSource> partitionLookupSource)
    {
        requireNonNull(partitionLookupSource, "partitionLookupSource is null");

        boolean completed;

        synchronized (this) {
            if (destroyed.isDone()) {
                return immediateVoidFuture();
            }

            checkState(partitions[partitionIndex] == null, "Partition already set");
            partitions[partitionIndex] = partitionLookupSource;
            partitionsSet++;
            completed = (partitionsSet == partitions.length);
        }

        if (completed) {
            supplyLookupSources();
        }

        return partitionsNoLongerNeeded;
    }

    private void supplyLookupSources()
    {
        List<SettableFuture<LookupSource>> lookupSourceFutures;
        TrackingLookupSourceSupplier lookupSourceSupplier;

        synchronized (this) {
            checkState(partitionsSet == partitions.length, "Not all set yet");
            checkState(this.lookupSourceSupplier == null, "Already supplied");

            if (partitionsNoLongerNeeded.isDone()) {
                return;
            }

            if (partitionsSet != 1) {
                List<Supplier<LookupSource>> partitions = ImmutableList.copyOf(this.partitions);
                lookupSourceSupplier = createPartitionedLookupSourceSupplier(partitions, hashChannelTypes, outer, blockTypeOperators);
            }
            else if (outer) {
                lookupSourceSupplier = createOuterLookupSourceSupplier(partitions[0]);
            }
            else {
                lookupSourceSupplier = TrackingLookupSourceSupplier.nonTracking(partitions[0]);
            }
            this.lookupSourceSupplier = lookupSourceSupplier;

            // store futures into local variables so they can be used outside of the lock
            lookupSourceFutures = ImmutableList.copyOf(this.lookupSourceFutures);
        }

        for (SettableFuture<LookupSource> lookupSourceFuture : lookupSourceFutures) {
            lookupSourceFuture.set(lookupSourceSupplier.getLookupSource());
        }
    }

    @Override
    public OuterPositionIterator getOuterPositionIterator()
    {
        TrackingLookupSourceSupplier lookupSourceSupplier;

        synchronized (this) {
            checkState(this.lookupSourceSupplier != null, "lookup source not ready yet");
            lookupSourceSupplier = this.lookupSourceSupplier;
        }

        return lookupSourceSupplier.getOuterPositionIterator();
    }

    @Override
    public synchronized void destroy()
    {
        freePartitions();

        // Setting destroyed must be last because it's a part of the state exposed by isDestroyed() without synchronization.
        destroyed.set(null);
    }

    private void freePartitions()
    {
        // Let the HashBuilderOperators reduce their accounted memory
        partitionsNoLongerNeeded.set(null);

        synchronized (this) {
            // Remove out references to partitions to actually free memory
            Arrays.fill(partitions, null);
            lookupSourceSupplier = null;
        }
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    public ListenableFuture<Void> isDestroyed()
    {
        return nonCancellationPropagating(destroyed);
    }
}
