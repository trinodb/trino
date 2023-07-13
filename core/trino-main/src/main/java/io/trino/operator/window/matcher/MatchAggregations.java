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
package io.trino.operator.window.matcher;

import io.airlift.slice.SizeOf;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.operator.window.pattern.MatchAggregation;
import io.trino.operator.window.pattern.MatchAggregation.MatchAggregationInstantiator;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;

class MatchAggregations
{
    private static final long INSTANCE_SIZE = instanceSize(MatchAggregations.class);

    private MatchAggregation[][] values;
    private final List<MatchAggregationInstantiator> aggregationInstantiators;
    private final AggregatedMemoryContext memoryContext;
    private long valuesSize; // only captures the structure size. MatchAggregations report memory on their own.
    private final long listSize;

    public MatchAggregations(int capacity, List<MatchAggregationInstantiator> aggregationInstantiators, AggregatedMemoryContext memoryContext)
    {
        this.values = new MatchAggregation[capacity][];
        this.aggregationInstantiators = aggregationInstantiators;
        this.memoryContext = memoryContext;
        this.valuesSize = 0L;
        this.listSize = SizeOf.sizeOf(new MatchAggregation[aggregationInstantiators.size()]);
    }

    public MatchAggregation[] get(int key)
    {
        if (values[key] == null) {
            MatchAggregation[] aggregations = new MatchAggregation[aggregationInstantiators.size()];
            for (int i = 0; i < aggregationInstantiators.size(); i++) {
                aggregations[i] = aggregationInstantiators.get(i).get(memoryContext);
                // no need to reset() when creating new MatchAggregation
                values[key] = aggregations;
            }
            valuesSize += listSize;
        }
        return values[key];
    }

    public void release(int key)
    {
        if (values[key] != null) {
            valuesSize -= listSize;
            values[key] = null;
        }
    }

    public void copy(int parent, int child)
    {
        ensureCapacity(child);
        checkState(values[child] == null, "overriding aggregations for child thread");

        if (values[parent] != null) {
            MatchAggregation[] aggregations = new MatchAggregation[aggregationInstantiators.size()];
            for (int i = 0; i < aggregationInstantiators.size(); i++) {
                aggregations[i] = values[parent][i].copy();
                values[child] = aggregations;
            }
            valuesSize += listSize;
        }
    }

    private void ensureCapacity(int key)
    {
        if (key >= values.length) {
            values = Arrays.copyOf(values, Math.max(values.length * 2, key + 1));
        }
    }

    public long getSizeInBytes()
    {
        // the size of MatchAggregationSuppliers and MatchAggregations is not reported here. MatchAggregations report memory usage.
        return INSTANCE_SIZE + SizeOf.sizeOf(values) + valuesSize;
    }
}
