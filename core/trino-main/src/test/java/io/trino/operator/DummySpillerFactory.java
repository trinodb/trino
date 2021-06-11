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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

public class DummySpillerFactory
        implements SpillerFactory
{
    private long spillsCount;

    @Override
    public Spiller create(List<Type> types, SpillContext spillContext, AggregatedMemoryContext memoryContext)
    {
        return new Spiller()
        {
            private final List<Iterable<Page>> spills = new ArrayList<>();

            @Override
            public ListenableFuture<Void> spill(Iterator<Page> pageIterator)
            {
                spillsCount++;
                spills.add(ImmutableList.copyOf(pageIterator));
                return immediateVoidFuture();
            }

            @Override
            public List<Iterator<Page>> getSpills()
            {
                return spills.stream()
                        .map(Iterable::iterator)
                        .collect(toImmutableList());
            }

            @Override
            public void close()
            {
                spills.clear();
            }
        };
    }

    public long getSpillsCount()
    {
        return spillsCount;
    }
}
