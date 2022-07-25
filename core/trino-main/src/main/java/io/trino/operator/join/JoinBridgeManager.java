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

package io.trino.operator.join;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.ReferenceCount;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class JoinBridgeManager<T extends JoinBridge>
{
    @VisibleForTesting
    public static JoinBridgeManager<PartitionedLookupSourceFactory> lookupAllAtOnce(PartitionedLookupSourceFactory factory)
    {
        return new JoinBridgeManager<>(
                false,
                factory,
                factory.getOutputTypes());
    }

    private final List<Type> buildOutputTypes;
    private final boolean buildOuter;
    private final T joinBridge;

    private final AtomicBoolean initialized = new AtomicBoolean();
    private JoinLifecycle joinLifecycle;

    private final FreezeOnReadCounter probeFactoryCount = new FreezeOnReadCounter();

    public JoinBridgeManager(
            boolean buildOuter,
            T joinBridge,
            List<Type> buildOutputTypes)
    {
        this.buildOuter = buildOuter;
        this.joinBridge = requireNonNull(joinBridge, "joinBridge is null");
        this.buildOutputTypes = requireNonNull(buildOutputTypes, "buildOutputTypes is null");
    }

    private void initializeIfNecessary()
    {
        if (!initialized.get()) {
            synchronized (this) {
                if (initialized.get()) {
                    return;
                }
                int finalProbeFactoryCount = probeFactoryCount.get();
                joinLifecycle = new JoinLifecycle(joinBridge, finalProbeFactoryCount, buildOuter ? 1 : 0);
                initialized.set(true);
            }
        }
    }

    public List<Type> getBuildOutputTypes()
    {
        return buildOutputTypes;
    }

    public void incrementProbeFactoryCount()
    {
        probeFactoryCount.increment();
    }

    public T getJoinBridge()
    {
        initializeIfNecessary();
        return joinBridge;
    }

    public void probeOperatorFactoryClosed()
    {
        initializeIfNecessary();
        joinLifecycle.releaseForProbe();
    }

    public void probeOperatorCreated()
    {
        initializeIfNecessary();
        joinLifecycle.retainForProbe();
    }

    public void probeOperatorClosed()
    {
        initializeIfNecessary();
        joinLifecycle.releaseForProbe();
    }

    public void outerOperatorFactoryClosed()
    {
        initializeIfNecessary();
        joinLifecycle.releaseForOuter();
    }

    public void outerOperatorCreated()
    {
        initializeIfNecessary();
        joinLifecycle.retainForOuter();
    }

    public void outerOperatorClosed()
    {
        initializeIfNecessary();
        joinLifecycle.releaseForOuter();
    }

    public ListenableFuture<OuterPositionIterator> getOuterPositionsFuture()
    {
        initializeIfNecessary();
        return transform(joinLifecycle.whenBuildAndProbeFinishes(), ignored -> joinBridge.getOuterPositionIterator(), directExecutor());
    }

    private static class JoinLifecycle
    {
        private final ReferenceCount probeReferenceCount;
        private final ReferenceCount outerReferenceCount;

        private final ListenableFuture<Void> whenBuildAndProbeFinishes;
        private final ListenableFuture<Void> whenAllFinishes;

        public JoinLifecycle(JoinBridge joinBridge, int probeFactoryCount, int outerFactoryCount)
        {
            // When all probe and lookup-outer operators finish, destroy the join bridge (freeing the memory)
            // * Each LookupOuterOperatorFactory count as 1
            //   * There is at most 1 LookupOuterOperatorFactory
            // * Each LookupOuterOperator count as 1
            checkArgument(outerFactoryCount == 0 || outerFactoryCount == 1);
            outerReferenceCount = new ReferenceCount(outerFactoryCount);

            // * Each probe operator factory count as 1
            // * Each probe operator count as 1
            probeReferenceCount = new ReferenceCount(probeFactoryCount);

            whenBuildAndProbeFinishes = Futures.whenAllSucceed(joinBridge.whenBuildFinishes(), probeReferenceCount.getFreeFuture()).call(() -> null, directExecutor());
            whenAllFinishes = Futures.whenAllSucceed(whenBuildAndProbeFinishes, outerReferenceCount.getFreeFuture()).call(() -> null, directExecutor());
            whenAllFinishes.addListener(joinBridge::destroy, directExecutor());
        }

        public ListenableFuture<Void> whenBuildAndProbeFinishes()
        {
            return whenBuildAndProbeFinishes;
        }

        private void retainForProbe()
        {
            probeReferenceCount.retain();
        }

        private void releaseForProbe()
        {
            probeReferenceCount.release();
        }

        private void retainForOuter()
        {
            outerReferenceCount.retain();
        }

        private void releaseForOuter()
        {
            outerReferenceCount.release();
        }
    }

    private static class FreezeOnReadCounter
    {
        private int count;
        private boolean frozen;

        public synchronized void increment()
        {
            checkState(!frozen, "Counter has been read");
            count++;
        }

        public synchronized int get()
        {
            frozen = true;
            return count;
        }
    }
}
