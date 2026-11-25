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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class TestJoinBridgeManager
{
    @Test
    void testDestroyWhenProbeFinishesBeforeBuild()
    {
        TestingJoinBridge joinBridge = new TestingJoinBridge();
        JoinBridgeManager<TestingJoinBridge> joinBridgeManager = new JoinBridgeManager<>(false, true, joinBridge, ImmutableList.of());
        joinBridgeManager.incrementProbeFactoryCount();
        joinBridgeManager.probeOperatorCreated();
        joinBridgeManager.probeOperatorFactoryClosed();
        assertThat(joinBridge.destroyed).isFalse();

        joinBridgeManager.probeOperatorClosed();
        assertThat(joinBridge.destroyed).isTrue();
    }

    @Test
    void testDestroyWaitsForBuildWhenBuildNeededAfterProbe()
    {
        TestingJoinBridge joinBridge = new TestingJoinBridge();
        JoinBridgeManager<TestingJoinBridge> joinBridgeManager = new JoinBridgeManager<>(false, false, joinBridge, ImmutableList.of());
        joinBridgeManager.incrementProbeFactoryCount();
        joinBridgeManager.probeOperatorFactoryClosed();
        assertThat(joinBridge.destroyed).isFalse();

        joinBridge.buildFinished.set(null);
        assertThat(joinBridge.destroyed).isTrue();
    }

    @Test
    void testBuildOuterDestroyWaitsForBuildAndOuter()
    {
        TestingJoinBridge joinBridge = new TestingJoinBridge();
        JoinBridgeManager<TestingJoinBridge> joinBridgeManager = new JoinBridgeManager<>(true, true, joinBridge, ImmutableList.of());
        joinBridgeManager.incrementProbeFactoryCount();
        joinBridgeManager.probeOperatorFactoryClosed();
        assertThat(joinBridge.destroyed).isFalse();

        joinBridge.buildFinished.set(null);
        assertThat(joinBridge.destroyed).isFalse();

        joinBridgeManager.outerOperatorFactoryClosed();
        assertThat(joinBridge.destroyed).isTrue();
    }

    private static class TestingJoinBridge
            implements JoinBridge
    {
        private final SettableFuture<Void> buildFinished = SettableFuture.create();
        private boolean destroyed;

        @Override
        public void destroy()
        {
            destroyed = true;
        }

        @Override
        public OuterPositionIterator getOuterPositionIterator()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<Void> whenBuildFinishes()
        {
            return buildFinished;
        }
    }
}
