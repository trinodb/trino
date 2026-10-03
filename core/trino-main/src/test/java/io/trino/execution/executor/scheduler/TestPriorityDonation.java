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
package io.trino.execution.executor.scheduler;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static io.trino.testing.assertions.Assert.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;

/// The priority-boost primitive donation is built on: a boosted node is pulled ahead of fair order
/// across nesting levels, and re-ranked or restored to fair order when the boost changes or clears.
public class TestPriorityDonation
{
    @Test
    public void testBoostPullsProducerToFrontOfGroup()
    {
        SchedulingNode<String> queue = new SchedulingNode<>();
        queue.startGroup(List.of("G"));
        queue.enqueue(List.of("G", "a"), 1);
        queue.enqueue(List.of("G", "b"), 2);
        queue.enqueue(List.of("G", "p"), 50); // far back in fair order

        assertThat(queue.peek()).isEqualTo("a");

        queue.setBoost(List.of("G", "p"), 0);
        assertThat(queue.dequeue(1)).isEqualTo("p"); // jumps the queue

        // The rest resume fair order.
        assertThat(queue.dequeue(1)).isEqualTo("a");
        assertThat(queue.dequeue(1)).isEqualTo("b");
    }

    @Test
    public void testClearBoostRestoresFairOrder()
    {
        SchedulingNode<String> queue = new SchedulingNode<>();
        queue.startGroup(List.of("G"));
        queue.enqueue(List.of("G", "a"), 1);
        queue.enqueue(List.of("G", "p"), 50);

        queue.setBoost(List.of("G", "p"), 0);
        assertThat(queue.peek()).isEqualTo("p");

        queue.clearBoost(List.of("G", "p"));
        assertThat(queue.peek()).isEqualTo("a"); // back to fair order
    }

    @Test
    public void testBoostCrossesGroupBoundaries()
    {
        // A producer in a heavier group would normally lose to a lighter group; boosting flips that.
        SchedulingNode<String> queue = new SchedulingNode<>();
        queue.startGroup(List.of("Light"));
        queue.startGroup(List.of("Heavy"));
        queue.enqueue(List.of("Light", "x"), 1);
        queue.enqueue(List.of("Heavy", "p"), 100);

        assertThat(queue.peek()).isEqualTo("x");

        queue.setBoost(List.of("Heavy", "p"), 0);
        assertThat(queue.dequeue(1)).isEqualTo("p");
    }

    @Test
    public void testMoreUrgentDonorWinsAmongBoosted()
    {
        // Two producers, both boosted. Fair order would take p1 (lighter) first, but p2's donor is
        // more urgent (lower rank), so p2 wins among the boosted pair.
        SchedulingNode<String> queue = new SchedulingNode<>();
        queue.startGroup(List.of("G"));
        queue.enqueue(List.of("G", "p1"), 10);
        queue.enqueue(List.of("G", "p2"), 20);

        assertThat(queue.peek()).isEqualTo("p1"); // fair order

        queue.setBoost(List.of("G", "p1"), 100); // boosted, but by a less urgent consumer
        queue.setBoost(List.of("G", "p2"), 50); // boosted by a more urgent consumer

        assertThat(queue.dequeue(1)).isEqualTo("p2");
        assertThat(queue.dequeue(1)).isEqualTo("p1");
    }

    @Test
    public void testBoostDoesNotStarveRunnableSibling()
    {
        // Priority inheritance, not an absolute band: a boosted producer starts at the donor's
        // virtual runtime but accrues from there, so a sibling that is already runnable is scheduled
        // within a bounded number of rounds instead of being starved forever. This is what keeps
        // donation from livelocking the work a blocked consumer ultimately depends on.
        SchedulingNode<String> queue = new SchedulingNode<>();
        queue.startGroup(List.of("G"));
        queue.enqueue(List.of("G", "p"), 0); // producer, fresh (low virtual runtime)
        queue.enqueue(List.of("G", "s"), 5); // sibling, already a little ahead in accumulated work

        queue.setBoost(List.of("G", "p"), 0); // donor at rank 0: p sorts first...
        assertThat(queue.peek()).isEqualTo("p");

        // ...but as p runs it accrues runtime and must yield to s rather than monopolizing every round.
        boolean sRan = false;
        for (int i = 0; i < 50 && !sRan; i++) {
            String task = queue.dequeue(10);
            queue.enqueue(List.of("G", task), 10); // busy task: consumes its whole quantum and returns
            sRan = task.equals("s");
        }
        assertThat(sRan).isTrue();
    }

    @Test
    public void testBoostingAGroupPullsItsWholeSubtreeToFront()
    {
        // Boosting a group repositions its whole subtree to the donor's virtual runtime: every task
        // under it inherits the boost and is pulled ahead of a more-deferred sibling group, which is
        // what pipeline-level donation relies on. The pull is by inheritance, so it lasts only until
        // the boosted subtree accrues past the sibling — here the sibling is deferred enough that the
        // subtree drains first.
        SchedulingNode<String> tree = new SchedulingNode<>();
        tree.startGroup(List.of("light"));
        tree.startGroup(List.of("heavy"));
        tree.enqueue(List.of("light", "x"), 40); // fair order would still run x first...
        tree.enqueue(List.of("heavy", "a"), 50);
        tree.enqueue(List.of("heavy", "b"), 60);

        assertThat(tree.peek()).isEqualTo("x");

        tree.setBoost(List.of("heavy"), 0); // ...but a rank-0 donor pulls the whole heavy subtree ahead
        assertThat(tree.dequeue(1)).isEqualTo("a");
        assertThat(tree.dequeue(1)).isEqualTo("b");
        assertThat(tree.dequeue(1)).isEqualTo("x");
    }

    @Test
    @Timeout(10)
    public void testBlockingOnAProducerPipelineBoostsIt()
            throws ExecutionException, InterruptedException
    {
        // A probe blocks on the build pipeline (a sibling group). While blocked, the whole build
        // group is boosted; the boost is withdrawn when the probe unblocks.
        try (FairScheduler scheduler = FairScheduler.newInstance(2)) {
            Group task = scheduler.createGroup("task");
            Group build = scheduler.createGroup(task, "build");
            Group probe = scheduler.createGroup(task, "probe");

            SettableFuture<Void> buildGate = SettableFuture.create();
            SettableFuture<Void> lookupSource = SettableFuture.create();
            CountDownLatch buildStarted = new CountDownLatch(1);

            ListenableFuture<Void> buildSplit = scheduler.submit(build, 1, context -> {
                buildStarted.countDown();
                context.block(buildGate);
            });
            buildStarted.await();

            ListenableFuture<Void> probeSplit = scheduler.submit(probe, 1, context -> context.blockOnProducerPipelines(lookupSource, Set.of(build)));

            assertEventually(() -> assertThat(scheduler.pipelineBoostCount(build)).isEqualTo(1));

            lookupSource.set(null); // probe's dependency satisfied
            assertEventually(() -> assertThat(scheduler.pipelineBoostCount(build)).isZero());

            buildGate.set(null);
            probeSplit.get();
            buildSplit.get();
        }
    }

    @Test
    @Timeout(10)
    public void testSeveralProbesComposeOnOneBuild()
            throws ExecutionException, InterruptedException
    {
        // Two probes block on one build. The build carries the donors' rank while both wait, and when
        // one unblocks it stays boosted at the remaining donor's rank rather than clearing — the
        // behaviour a multiset has that a plain counter (no rank) or a last-writer (one value, cleared
        // on any release) does not.
        try (FairScheduler scheduler = FairScheduler.newInstance(1)) {
            Group task = scheduler.createGroup("task");
            Group build = scheduler.createGroup(task, "build");
            Group probes = scheduler.createGroup(task, "probes");

            ListenerFuture buildGate = new ListenerFuture();
            ListenerFuture data1 = new ListenerFuture();
            ListenerFuture data2 = new ListenerFuture();

            // Build parks so its group exists and stays alive to be boosted.
            scheduler.submit(build, 1, context -> context.block(buildGate));
            buildGate.awaitListenerAdded();

            scheduler.submit(probes, 1, context -> context.blockOnProducerPipelines(data1, Set.of(build)));
            data1.awaitListenerAdded();
            long rank = scheduler.pipelineBoostRank(build);

            scheduler.submit(probes, 2, context -> context.blockOnProducerPipelines(data2, Set.of(build)));
            data2.awaitListenerAdded();

            // Both donors are held at once, and the build carries a real rank (a counter has none).
            assertThat(scheduler.pipelineBoostCount(build)).isEqualTo(2);
            assertThat(scheduler.pipelineBoostRank(build)).isEqualTo(rank);

            // One probe unblocks: the build is still boosted by the other, at a real rank — not cleared
            // (a last-writer would have kept a single value and dropped it here).
            data1.set();
            assertEventually(() -> assertThat(scheduler.pipelineBoostCount(build)).isEqualTo(1));
            assertThat(scheduler.pipelineBoostRank(build)).isEqualTo(rank);

            data2.set();
            buildGate.set();
        }
    }

    @Test
    @Timeout(10)
    public void testDonationToManyProducersIsCapped()
            throws ExecutionException, InterruptedException
    {
        // A consumer that names more producers than the scheduler has slots boosts only a slot's
        // worth of them, so a wide exchange fan-in cannot flood the priority band.
        int slots = 2;
        int producerCount = 5;
        try (FairScheduler scheduler = FairScheduler.newInstance(slots)) {
            Group consumerGroup = scheduler.createGroup("consumer");

            List<Group> producers = new ArrayList<>();
            List<ListenerFuture> gates = new ArrayList<>();
            for (int i = 0; i < producerCount; i++) {
                Group producer = scheduler.createGroup("producer-" + i);
                producers.add(producer);
                ListenerFuture gate = new ListenerFuture();
                gates.add(gate);
                // Each producer parks a split so its group exists and stays alive to be boosted.
                scheduler.submit(producer, 1, context -> context.block(gate));
            }
            for (ListenerFuture gate : gates) {
                gate.awaitListenerAdded();
            }

            ListenerFuture data = new ListenerFuture();
            scheduler.submit(consumerGroup, 1, context -> context.blockOnProducerPipelines(data, producers));
            data.awaitListenerAdded();

            int boosted = producers.stream().mapToInt(scheduler::pipelineBoostCount).sum();
            assertThat(boosted).isEqualTo(slots);

            data.set();
            for (ListenerFuture gate : gates) {
                gate.set();
            }
        }
    }

    /// A future that reveals when the scheduler has registered its unblock listener — i.e. when the
    /// blocking task has been frozen out of the scheduling tree and its donation applied — so a test
    /// can read the resulting boost deterministically.
    private static final class ListenerFuture
            extends AbstractFuture<Void>
    {
        private final CountDownLatch listenerAdded = new CountDownLatch(1);

        @Override
        public void addListener(Runnable listener, Executor executor)
        {
            super.addListener(listener, executor);
            listenerAdded.countDown();
        }

        void set()
        {
            super.set(null);
        }

        void awaitListenerAdded()
                throws InterruptedException
        {
            listenerAdded.await();
        }
    }
}
