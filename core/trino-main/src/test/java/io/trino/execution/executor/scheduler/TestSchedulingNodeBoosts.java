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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSchedulingNodeBoosts
{
    @Test
    public void testClearingSiblingBoostKeepsSharedAncestorBoosted()
    {
        SchedulingNode<String> tree = createTree();
        tree.setBoost(List.of("heavy", "p1"), 0);
        tree.setBoost(List.of("heavy", "p2"), 0);

        tree.clearBoost(List.of("heavy", "p1"));
        assertThat(tree.peek()).isEqualTo("b");
        tree.clearBoost(List.of("heavy", "p2"));
        assertThat(tree.peek()).isEqualTo("x");
    }

    @Test
    public void testLessUrgentDonationKeepsSharedAncestorPriority()
    {
        SchedulingNode<String> tree = createTree();
        tree.setBoost(List.of("heavy", "p1"), 0);
        tree.setBoost(List.of("heavy", "p2"), 20);

        assertThat(tree.peek()).isEqualTo("a");
    }

    @Test
    public void testRerankingOneDonationPreservesOtherDonation()
    {
        SchedulingNode<String> tree = createTree();
        tree.setBoost(List.of("heavy", "p1"), 0);
        tree.setBoost(List.of("heavy", "p2"), 5);

        tree.setBoost(List.of("heavy", "p1"), 20);
        assertThat(tree.peek()).isEqualTo("b");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testAncestorAndDescendantDonationsCompose(boolean clearAncestor)
    {
        SchedulingNode<String> tree = createTree();
        tree.setBoost(List.of("heavy"), 0);
        tree.setBoost(List.of("heavy", "p2"), 0);

        tree.clearBoost(clearAncestor ? List.of("heavy") : List.of("heavy", "p2"));
        assertThat(tree.peek()).isIn("a", "b");
        tree.clearBoost(clearAncestor ? List.of("heavy", "p2") : List.of("heavy"));
        assertThat(tree.peek()).isEqualTo("x");
    }

    @Test
    public void testRemainingDonationKeepsAccruedRuntime()
    {
        SchedulingNode<String> tree = createTree();
        tree.setBoost(List.of("heavy", "p1"), 0);
        tree.setBoost(List.of("heavy", "p2"), 0);
        assertThat(tree.dequeue(100)).isEqualTo("a");
        tree.enqueue(List.of("heavy", "p1", "a"), 100);

        tree.clearBoost(List.of("heavy", "p1"));
        assertThat(tree.peek()).isEqualTo("x");
        tree.setBoost(List.of("heavy", "p2"), 0);
        assertThat(tree.peek()).isEqualTo("x");
    }

    @Test
    public void testLaterDonationHasIndependentAccrual()
    {
        SchedulingNode<String> tree = createTree();
        tree.setBoost(List.of("heavy", "p1"), 0);
        assertThat(tree.dequeue(100)).isEqualTo("a");
        tree.enqueue(List.of("heavy", "p1", "a"), 100);

        tree.setBoost(List.of("heavy", "p2"), 20);
        assertThat(tree.peek()).isEqualTo("x");
        tree.clearBoost(List.of("heavy", "p1"));
        assertThat(tree.peek()).isEqualTo("x");
        tree.setBoost(List.of("heavy", "p2"), 0);
        assertThat(tree.peek()).isEqualTo("b");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRemovingBoostedSubtreeClearsDonations(boolean boostLeaf)
    {
        SchedulingNode<String> tree = createTree();
        tree.setBoost(boostLeaf ? List.of("heavy", "p1", "a") : List.of("heavy", "p1"), 0);
        assertThat(tree.peek()).isEqualTo("a");

        assertThat(tree.finishGroup(List.of("heavy", "p1"))).containsExactly("a");
        assertThat(tree.peek()).isEqualTo("x");
    }

    @Test
    public void testFinishingBoostedLeafClearsDonation()
    {
        SchedulingNode<String> tree = createTree();
        tree.setBoost(List.of("heavy", "p1", "a"), 0);
        tree.finish(List.of("heavy", "p1", "a"));

        assertThat(tree.peek()).isEqualTo("x");
    }

    @Test
    public void testRemovingBoostedSubtreePreservesSiblingDonation()
    {
        SchedulingNode<String> tree = createTree();
        tree.setBoost(List.of("heavy", "p1"), 0);
        tree.setBoost(List.of("heavy", "p2"), 0);
        tree.finishGroup(List.of("heavy", "p1"));

        assertThat(tree.peek()).isEqualTo("b");
        tree.clearBoost(List.of("heavy", "p2"));
        assertThat(tree.peek()).isEqualTo("x");
    }

    @Test
    public void testMissingProducerDoesNotChangeAncestorDonations()
    {
        SchedulingNode<String> tree = createTree();
        tree.setBoost(List.of("heavy", "missing"), 0);
        assertThat(tree.peek()).isEqualTo("x");

        tree.setBoost(List.of("heavy", "p1"), 0);
        tree.clearBoost(List.of("heavy", "missing"));
        assertThat(tree.peek()).isEqualTo("a");
    }

    private static SchedulingNode<String> createTree()
    {
        SchedulingNode<String> tree = new SchedulingNode<>();
        tree.startGroup(List.of("light"));
        tree.startGroup(List.of("heavy"));
        tree.startGroup(List.of("heavy", "p1"));
        tree.startGroup(List.of("heavy", "p2"));
        tree.enqueue(List.of("light", "x"), 10);
        tree.enqueue(List.of("heavy", "p1", "a"), 100);
        tree.enqueue(List.of("heavy", "p2", "b"), 100);
        assertThat(tree.peek()).isEqualTo("x");
        return tree;
    }
}
