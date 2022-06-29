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
import io.trino.operator.join.LookupSource;
import io.trino.operator.join.OuterPositionIterator;
import io.trino.operator.join.TrackingLookupSourceSupplier;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
        implements LookupSourceFactory
{
    private final List<Type> types;
    private final List<Type> outputTypes;
    private final List<Type> hashChannelTypes;
    private final boolean outer;
    private final BlockTypeOperators blockTypeOperators;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @GuardedBy("lock")
    private final Supplier<LookupSource>[] partitions;

    private final SettableFuture<Void> partitionsNoLongerNeeded = SettableFuture.create();

    @GuardedBy("lock")
    private final SettableFuture<Void> destroyed = SettableFuture.create();

    @GuardedBy("lock")
    private int partitionsSet;

    @GuardedBy("lock")
    private TrackingLookupSourceSupplier lookupSourceSupplier;

    @GuardedBy("lock")
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

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    // partitions is final, so we don't need a lock to read its length here
    @SuppressWarnings("FieldAccessNotGuarded")
    @Override
    public int partitions()
    {
        return partitions.length;
    }

    @Override
    public ListenableFuture<LookupSource> createLookupSource()
    {
        lock.writeLock().lock();
        try {
            checkState(!destroyed.isDone(), "already destroyed");
            if (lookupSourceSupplier != null) {
                return immediateFuture(lookupSourceSupplier.getLookupSource());
            }

            SettableFuture<LookupSource> lookupSourceFuture = SettableFuture.create();
            lookupSourceFutures.add(lookupSourceFuture);
            return lookupSourceFuture;
        }
        finally {
            lock.writeLock().unlock();
        }
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

        lock.writeLock().lock();
        try {
            if (destroyed.isDone()) {
                return immediateVoidFuture();
            }

            checkState(partitions[partitionIndex] == null, "Partition already set");
            partitions[partitionIndex] = partitionLookupSource;
            partitionsSet++;
            completed = (partitionsSet == partitions.length);
        }
        finally {
            lock.writeLock().unlock();
        }

        if (completed) {
            supplyLookupSources();
        }

        return partitionsNoLongerNeeded;
    }

    private void supplyLookupSources()
    {
        checkState(!lock.isWriteLockedByCurrentThread());

        List<SettableFuture<LookupSource>> lookupSourceFutures;
        TrackingLookupSourceSupplier lookupSourceSupplier;

        lock.writeLock().lock();
        try {
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
        finally {
            lock.writeLock().unlock();
        }

        for (SettableFuture<LookupSource> lookupSourceFuture : lookupSourceFutures) {
            lookupSourceFuture.set(lookupSourceSupplier.getLookupSource());
        }
    }

    @Override
    public OuterPositionIterator getOuterPositionIterator()
    {
        TrackingLookupSourceSupplier lookupSourceSupplier;

        lock.writeLock().lock();
        try {
            checkState(this.lookupSourceSupplier != null, "lookup source not ready yet");
            lookupSourceSupplier = this.lookupSourceSupplier;
        }
        finally {
            lock.writeLock().unlock();
        }

        return lookupSourceSupplier.getOuterPositionIterator();
    }

    @Override
    public void destroy()
    {
        lock.writeLock().lock();
        try {
            freePartitions();

            // Setting destroyed must be last because it's a part of the state exposed by isDestroyed() without synchronization.
            destroyed.set(null);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    private void freePartitions()
    {
        // Let the HashBuilderOperators reduce their accounted memory
        partitionsNoLongerNeeded.set(null);

        lock.writeLock().lock();
        try {
            // Remove out references to partitions to actually free memory
            Arrays.fill(partitions, null);
            lookupSourceSupplier = null;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    @Override
    public ListenableFuture<Void> isDestroyed()
    {
        return nonCancellationPropagating(destroyed);
    }
}
