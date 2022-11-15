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
package io.trino.plugin.hive.statistics;

import com.google.common.base.Stopwatch;
import com.google.common.collect.AbstractIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.google.common.collect.Iterators.addAll;
import static com.google.common.collect.Iterators.limit;

public final class TimedBatching
{
    private TimedBatching() {}

    public static <T, R> Iterator<R> processBatchesWithApproximateTimeLimit(
            Iterator<T> items,
            int initialBatchSize,
            Function<List<T>, R> processor,
            long approximateTimeLimit,
            TimeUnit timeUnit)
    {
        return new AbstractIterator<R>()
        {
            private final Stopwatch stopwatch = Stopwatch.createStarted();
            int batchSize = initialBatchSize;
            private final List<T> batch = new ArrayList<>(batchSize);

            @Override
            protected R computeNext()
            {
                if (!items.hasNext() || stopwatch.elapsed(timeUnit) >= approximateTimeLimit) {
                    return endOfData();
                }

                take(items, batch, batchSize);
                R result = processor.apply(batch);
                if (stopwatch.elapsed(timeUnit) >= approximateTimeLimit / 4) {
                    batchSize *= 2;
                }
                return result;
            }
        };
    }

    private static <T> void take(Iterator<T> source, List<T> target, int limit)
    {
        target.clear();
        addAll(target, limit(source, limit));
    }
}
