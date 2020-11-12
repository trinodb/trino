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
package io.trino.spi.metrics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.trino.spi.Mergeable;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class Metrics
        implements Mergeable<Metrics>
{
    public static final Metrics EMPTY = new Metrics(Map.of());

    private final Map<String, Metric> metrics;

    @JsonCreator
    public Metrics(Map<String, Metric> metrics)
    {
        this.metrics = Map.copyOf(requireNonNull(metrics, "metrics is null"));
    }

    @JsonValue
    public Map<String, Metric> getMetrics()
    {
        return metrics;
    }

    @Override
    public Metrics mergeWith(Metrics other)
    {
        return accumulator().add(this).add(other).get();
    }

    public static Accumulator accumulator()
    {
        return new Accumulator();
    }

    public static class Accumulator
    {
        private final Map<String, Metric> merged = new HashMap<>();

        private Accumulator()
        {
        }

        public Accumulator add(Metrics metrics)
        {
            metrics.getMetrics().forEach((key, value) ->
                    merged.merge(key, value, (left, right) -> (Metric) left.mergeWith(right)));
            return this;
        }

        public Metrics get()
        {
            return new Metrics(merged);
        }
    }
}
