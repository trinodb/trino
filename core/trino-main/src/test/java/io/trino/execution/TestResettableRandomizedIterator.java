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
package io.trino.execution;

import com.google.common.collect.ImmutableSet;
import io.trino.execution.scheduler.ResettableRandomizedIterator;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestResettableRandomizedIterator
{
    @Test
    public void testResetting()
    {
        Set<Integer> expected = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            expected.add(i);
        }
        expected = ImmutableSet.copyOf(expected);

        ResettableRandomizedIterator<Integer> randomizedIterator = new ResettableRandomizedIterator<>(expected);

        Set<Integer> actual = new HashSet<>();
        while (randomizedIterator.hasNext()) {
            actual.add(randomizedIterator.next());
        }
        assertThat(actual).hasSameElementsAs(expected);

        actual.clear();
        randomizedIterator.reset();
        while (randomizedIterator.hasNext()) {
            actual.add(randomizedIterator.next());
        }
        assertThat(actual).hasSameElementsAs(expected);
    }

    @Test
    public void testRandom()
    {
        Set<Integer> values = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            values.add(i);
        }
        values = ImmutableSet.copyOf(values);

        ResettableRandomizedIterator<Integer> randomizedIterator = new ResettableRandomizedIterator<>(values);

        List<Integer> list1 = new ArrayList<>();
        List<Integer> list2 = new ArrayList<>();
        randomizedIterator.reset();
        for (int i = 0; i < 99; i++) {
            list1.add(randomizedIterator.next());
        }
        randomizedIterator.reset();
        for (int i = 0; i < 99; i++) {
            list2.add(randomizedIterator.next());
        }
        assertThat(list1).isNotEqualTo(list2);
    }
}
