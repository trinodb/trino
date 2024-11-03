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

import static java.util.Objects.requireNonNull;

public class MeasuredGroupByHashWork<T>
        implements Work<T>
{
    private final Work<T> delegate;
    private final AggregationMetrics metrics;

    public MeasuredGroupByHashWork(Work<T> delegate, AggregationMetrics metrics)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.metrics = requireNonNull(metrics, "metrics is null");
    }

    @Override
    public boolean process()
    {
        long start = System.nanoTime();
        boolean result = delegate.process();
        metrics.recordGroupByHashUpdateTimeSince(start);
        return result;
    }

    @Override
    public T getResult()
    {
        return delegate.getResult();
    }
}
