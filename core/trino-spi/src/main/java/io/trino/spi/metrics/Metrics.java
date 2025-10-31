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
import io.trino.spi.Unstable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class Metrics
        implements Mergeable<Metrics>
{
    public static final Metrics EMPTY = new Metrics(Map.of());

    private final Map<String, Metric<?>> metrics;

    @JsonCreator
    @Unstable
    public Metrics(Map<String, Metric<?>> metrics)
    {
        this.metrics = Map.copyOf(requireNonNull(metrics, "metrics is null"));
    }

    @JsonValue
    public Map<String, Metric<?>> getMetrics()
    {
        return metrics;
    }

    public Metric<?> getMetric(String key)
    {
        return metrics.get(key);
    }

    public Set<String> getKeys()
    {
        return Set.copyOf(metrics.keySet());
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Metrics that)) {
            return false;
        }
        return metrics.equals(that.metrics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(metrics);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", Metrics.class.getSimpleName() + "[", "]")
                .add(metrics.toString())
                .toString();
    }

    public static class Accumulator
    {
        private final List<Metrics> values = new ArrayList<>(0);

        private Accumulator() {}

        public Accumulator add(Metrics metrics)
        {
            values.add(metrics);
            return this;
        }

        public Metrics get()
        {
            if (values.isEmpty()) {
                return EMPTY;
            }

            Set<String> keys = values.stream()
                    .flatMap(metric -> metric.getKeys().stream())
                    .collect(toSet());

            Map<String, Metric<?>> merged = new HashMap<>(keys.size());

            for (String key : keys) {
                List<Metric<?>> toMerge = new ArrayList<>();
                for (Metrics metrics : values) {
                    Metric<?> metric = metrics.getMetric(key);
                    if (metric != null) {
                        toMerge.add(metric);
                    }
                }
                merged.put(key, merge(toMerge));
            }

            return new Metrics(merged);
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private static Metric<?> merge(List<Metric<?>> values)
        {
            Metric<?> head = values.getFirst();
            if (values.size() == 1) {
                return head;
            }
            List<Metric<?>> tail = values.subList(1, values.size());
            return (Metric<?>) ((Metric) head).mergeWith(tail);
        }
    }
}
