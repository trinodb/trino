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
package io.trino.util;

import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDisjointSet
{
    @Test
    public void testInitial()
    {
        DisjointSet<Integer> disjoint = new DisjointSet<>();

        // assert that every node is considered its own group
        for (int i = 0; i < 100; i++) {
            assertThat(disjoint.find(i).intValue()).isEqualTo(i);
        }
        assertThat(disjoint.getEquivalentClasses().size()).isEqualTo(100);
    }

    @Test
    public void testMergeAllSequentially()
    {
        DisjointSet<Integer> disjoint = new DisjointSet<>();

        // insert pair (i, i+1); assert all inserts are considered new
        for (int i = 0; i < 100; i++) {
            assertThat(disjoint.findAndUnion(i, i + 1)).isTrue();
            if (i != 0) {
                assertThat(disjoint.find(i - 1)).isEqualTo(disjoint.find(i));
            }
            if (i != 99) {
                assertThat(disjoint.find(i + 1))
                        .isNotEqualTo(disjoint.find(i + 2));
            }
        }
        // assert every pair (i, j) is in the same set
        for (int i = 0; i <= 100; i++) {
            for (int j = 0; j <= 100; j++) {
                assertThat(disjoint.find(i)).isEqualTo(disjoint.find(j));
                assertThat(disjoint.findAndUnion(i, j)).isFalse();
            }
        }
        Collection<Set<Integer>> equivalentClasses = disjoint.getEquivalentClasses();
        assertThat(equivalentClasses.size()).isEqualTo(1);
        assertThat(Iterables.getOnlyElement(equivalentClasses).size()).isEqualTo(101);
    }

    @Test
    public void testMergeAllBackwardsSequentially()
    {
        DisjointSet<Integer> disjoint = new DisjointSet<>();

        // insert pair (i, i+1); assert all inserts are considered new
        for (int i = 100; i > 0; i--) {
            assertThat(disjoint.findAndUnion(i, i - 1)).isTrue();
            if (i != 100) {
                assertThat(disjoint.find(i + 1)).isEqualTo(disjoint.find(i));
            }
            if (i != 1) {
                assertThat(disjoint.find(i - 1))
                        .isNotEqualTo(disjoint.find(i - 2));
            }
        }
        // assert every pair (i, j) is in the same set
        for (int i = 0; i <= 100; i++) {
            for (int j = 0; j <= 100; j++) {
                assertThat(disjoint.find(i)).isEqualTo(disjoint.find(j));
                assertThat(disjoint.findAndUnion(i, j)).isFalse();
            }
        }
        Collection<Set<Integer>> equivalentClasses = disjoint.getEquivalentClasses();
        assertThat(equivalentClasses.size()).isEqualTo(1);
        assertThat(Iterables.getOnlyElement(equivalentClasses).size()).isEqualTo(101);
    }

    @Test
    public void testMergeFourGroups()
    {
        DisjointSet<Integer> disjoint = new DisjointSet<>();

        // insert pair (i, i+1); assert all inserts are considered new
        List<Integer> inputs = IntStream.range(0, 96).boxed().collect(Collectors.toList());
        Collections.shuffle(inputs);
        for (int i : inputs) {
            assertThat(disjoint.findAndUnion(i, i + 4)).isTrue();
        }
        // assert every pair (i, j) is in the same set
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 100; j++) {
                if ((i - j) % 4 == 0) {
                    assertThat(disjoint.find(i)).isEqualTo(disjoint.find(j));
                    assertThat(disjoint.findAndUnion(i, j)).isFalse();
                }
                else {
                    assertThat(disjoint.find(i))
                            .isNotEqualTo(disjoint.find(j));
                }
            }
        }
        Collection<Set<Integer>> equivalentClasses = disjoint.getEquivalentClasses();
        assertThat(equivalentClasses.size()).isEqualTo(4);
        equivalentClasses.forEach(equivalentClass -> assertThat(equivalentClass.size()).isEqualTo(25));
    }

    @Test
    public void testMergeRandomly()
    {
        DisjointSet<Double> disjoint = new DisjointSet<>();

        Random rand = new Random();
        // Don't use List here. That will box the primitive early.
        // Boxing need to happen right before calls to DisjointSet to test that correct equality check is used for comparisons.
        double[] numbers = new double[100];
        for (int i = 0; i < 100; i++) {
            numbers[i] = rand.nextDouble();
            disjoint.find(numbers[i]);
        }
        int groupCount = 100;
        while (groupCount > 1) {
            // this loop roughly runs 180-400 times
            boolean newEquivalence = disjoint.findAndUnion(numbers[rand.nextInt(100)], numbers[rand.nextInt(100)]);
            if (newEquivalence) {
                groupCount--;
            }
            assertThat(disjoint.getEquivalentClasses().size()).isEqualTo(groupCount);
        }
    }
}
