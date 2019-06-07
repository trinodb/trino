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

package io.prestosql.operator;

import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import org.openjdk.jol.info.ClassLayout;

import java.util.Comparator;

import static io.airlift.slice.SizeOf.sizeOf;

// this class is for precise memory tracking
public class RowHeap
        extends ObjectHeapPriorityQueue<Row>
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(RowHeap.class).instanceSize();
    private static final long ROW_ENTRY_SIZE = ClassLayout.parseClass(Row.class).instanceSize();

    public RowHeap(Comparator<Row> comparator)
    {
        super(1, comparator);
    }

    public long getEstimatedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(heap) + size() * ROW_ENTRY_SIZE;
    }
}
