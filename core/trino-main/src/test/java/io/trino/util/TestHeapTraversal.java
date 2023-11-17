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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHeapTraversal
{
    @Test
    public void testTraversal()
    {
        HeapTraversal heapTraversal = new HeapTraversal();

        /**
         * Node indexing scheme:
         *             Node1
         *          /        \
         *       Node2       Node3
         *      /    \       /
         *   Node4 Node5  Node6 ...
         */

        heapTraversal.resetWithPathTo(1);
        assertThat(heapTraversal.isTarget()).isTrue();

        heapTraversal.resetWithPathTo(2);
        assertThat(heapTraversal.isTarget()).isFalse();
        assertThat(heapTraversal.nextChild()).isEqualTo(HeapTraversal.Child.LEFT);
        assertThat(heapTraversal.isTarget()).isTrue();

        heapTraversal.resetWithPathTo(3);
        assertThat(heapTraversal.isTarget()).isFalse();
        assertThat(heapTraversal.nextChild()).isEqualTo(HeapTraversal.Child.RIGHT);
        assertThat(heapTraversal.isTarget()).isTrue();

        heapTraversal.resetWithPathTo(4);
        assertThat(heapTraversal.isTarget()).isFalse();
        assertThat(heapTraversal.nextChild()).isEqualTo(HeapTraversal.Child.LEFT);
        assertThat(heapTraversal.isTarget()).isFalse();
        assertThat(heapTraversal.nextChild()).isEqualTo(HeapTraversal.Child.LEFT);
        assertThat(heapTraversal.isTarget()).isTrue();

        heapTraversal.resetWithPathTo(5);
        assertThat(heapTraversal.isTarget()).isFalse();
        assertThat(heapTraversal.nextChild()).isEqualTo(HeapTraversal.Child.LEFT);
        assertThat(heapTraversal.isTarget()).isFalse();
        assertThat(heapTraversal.nextChild()).isEqualTo(HeapTraversal.Child.RIGHT);
        assertThat(heapTraversal.isTarget()).isTrue();

        heapTraversal.resetWithPathTo(6);
        assertThat(heapTraversal.isTarget()).isFalse();
        assertThat(heapTraversal.nextChild()).isEqualTo(HeapTraversal.Child.RIGHT);
        assertThat(heapTraversal.isTarget()).isFalse();
        assertThat(heapTraversal.nextChild()).isEqualTo(HeapTraversal.Child.LEFT);
        assertThat(heapTraversal.isTarget()).isTrue();
    }
}
