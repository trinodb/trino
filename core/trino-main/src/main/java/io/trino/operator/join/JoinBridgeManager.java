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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.operator.ReferenceCount;
import io.trino.operator.join.spilling.PartitionedLookupSourceFactory;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
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
                true,
                factory,
                factory.getOutputTypes());
    }

    private final List<Type> buildOutputTypes;
    private final boolean buildNotNeededAfterProbe;
    private final T joinBridge;
    private final JoinLifecycle joinLifecycle;

    @GuardedBy("this")
    private boolean probeFactoriesFrozen;

    /**
     * @param buildNotNeededAfterProbe when set, the join bridge is destroyed as soon as all probe operators
     *         finish, without waiting for the build to finish, unless the join is build-outer. Build operators then
     *         stop consuming input.
     */
    public JoinBridgeManager(
            boolean buildOuter,
            boolean buildNotNeededAfterProbe,
            T joinBridge,
            List<Type> buildOutputTypes)
    {
        this.joinBridge = requireNonNull(joinBridge, "joinBridge is null");
        this.buildOutputTypes = requireNonNull(buildOutputTypes, "buildOutputTypes is null");
        this.buildNotNeededAfterProbe = buildNotNeededAfterProbe && !buildOuter;
        // The probe reference count starts at 1 to act as a bootstrap reference that keeps
        // the bridge alive while probe operator factories are still being created. The
        // bootstrap reference is released on the first use of the bridge or any of its
        // lifecycle methods, at which point no more probe factories may be added.
        this.joinLifecycle = new JoinLifecycle(joinBridge, 1, buildOuter ? 1 : 0, this.buildNotNeededAfterProbe);
    }

    private synchronized void freezeProbeFactoriesIfNecessary()
    {
        if (!probeFactoriesFrozen) {
            probeFactoriesFrozen = true;
            joinLifecycle.releaseForProbe();
        }
    }

    public List<Type> getBuildOutputTypes()
    {
        return buildOutputTypes;
    }

    /**
     * Whether the build may stop before it finished, because all probe operators finished
     */
    public boolean isBuildNotNeededAfterProbe()
    {
        return buildNotNeededAfterProbe;
    }

    /**
     * Completes once the join bridge is destroyed, as no operator reads the build anymore
     */
    public ListenableFuture<Void> whenDestroyed()
    {
        return nonCancellationPropagating(joinLifecycle.whenAllFinishes());
    }

    public synchronized void incrementProbeFactoryCount()
    {
        checkState(!probeFactoriesFrozen, "Probe factories have been frozen");
        joinLifecycle.retainForProbe();
    }

    public T getJoinBridge()
    {
        freezeProbeFactoriesIfNecessary();
        return joinBridge;
    }

    public void probeOperatorFactoryClosed()
    {
        freezeProbeFactoriesIfNecessary();
        joinLifecycle.releaseForProbe();
    }

    public void probeOperatorCreated()
    {
        freezeProbeFactoriesIfNecessary();
        joinLifecycle.retainForProbe();
    }

    public void probeOperatorClosed()
    {
        freezeProbeFactoriesIfNecessary();
        joinLifecycle.releaseForProbe();
    }

    public void outerOperatorFactoryClosed()
    {
        freezeProbeFactoriesIfNecessary();
        joinLifecycle.releaseForOuter();
    }

    public void outerOperatorCreated()
    {
        freezeProbeFactoriesIfNecessary();
        joinLifecycle.retainForOuter();
    }

    public void outerOperatorClosed()
    {
        freezeProbeFactoriesIfNecessary();
        joinLifecycle.releaseForOuter();
    }

    public ListenableFuture<OuterPositionIterator> getOuterPositionsFuture()
    {
        freezeProbeFactoriesIfNecessary();
        return transform(joinLifecycle.whenBuildAndProbeFinishes(), _ -> joinBridge.getOuterPositionIterator(), directExecutor());
    }

    private static class JoinLifecycle
    {
        private final ReferenceCount probeReferenceCount;
        private final ReferenceCount outerReferenceCount;

        private final ListenableFuture<Void> whenBuildAndProbeFinishes;
        private final ListenableFuture<Void> whenAllFinishes;

        public JoinLifecycle(JoinBridge joinBridge, int probeFactoryCount, int outerFactoryCount, boolean buildNotNeededAfterProbe)
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
            if (buildNotNeededAfterProbe) {
                // Without a build-outer side, nothing reads the build once all probe operators finish,
                // even if the build has not finished yet
                checkArgument(outerFactoryCount == 0, "Build-outer join needs the whole build");
                whenAllFinishes = probeReferenceCount.getFreeFuture();
            }
            else {
                whenAllFinishes = Futures.whenAllSucceed(whenBuildAndProbeFinishes, outerReferenceCount.getFreeFuture()).call(() -> null, directExecutor());
            }
            whenAllFinishes.addListener(joinBridge::destroy, directExecutor());
        }

        public ListenableFuture<Void> whenAllFinishes()
        {
            return whenAllFinishes;
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
}
