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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
        assertTrue(heapTraversal.isTarget());

        heapTraversal.resetWithPathTo(2);
        assertFalse(heapTraversal.isTarget());
        assertEquals(heapTraversal.nextChild(), HeapTraversal.Child.LEFT);
        assertTrue(heapTraversal.isTarget());

        heapTraversal.resetWithPathTo(3);
        assertFalse(heapTraversal.isTarget());
        assertEquals(heapTraversal.nextChild(), HeapTraversal.Child.RIGHT);
        assertTrue(heapTraversal.isTarget());

        heapTraversal.resetWithPathTo(4);
        assertFalse(heapTraversal.isTarget());
        assertEquals(heapTraversal.nextChild(), HeapTraversal.Child.LEFT);
        assertFalse(heapTraversal.isTarget());
        assertEquals(heapTraversal.nextChild(), HeapTraversal.Child.LEFT);
        assertTrue(heapTraversal.isTarget());

        heapTraversal.resetWithPathTo(5);
        assertFalse(heapTraversal.isTarget());
        assertEquals(heapTraversal.nextChild(), HeapTraversal.Child.LEFT);
        assertFalse(heapTraversal.isTarget());
        assertEquals(heapTraversal.nextChild(), HeapTraversal.Child.RIGHT);
        assertTrue(heapTraversal.isTarget());

        heapTraversal.resetWithPathTo(6);
        assertFalse(heapTraversal.isTarget());
        assertEquals(heapTraversal.nextChild(), HeapTraversal.Child.RIGHT);
        assertFalse(heapTraversal.isTarget());
        assertEquals(heapTraversal.nextChild(), HeapTraversal.Child.LEFT);
        assertTrue(heapTraversal.isTarget());
    }
}
