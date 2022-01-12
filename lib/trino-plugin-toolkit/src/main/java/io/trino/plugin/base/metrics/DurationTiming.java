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
package io.trino.plugin.base.metrics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;
import io.trino.spi.metrics.Timing;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class DurationTiming
        implements Timing<DurationTiming>
{
    // use Airlift duration for more human-friendly serialization format
    // and to match duration serialization in other JSON objects
    private final Duration duration;

    @JsonCreator
    public DurationTiming(Duration duration)
    {
        this.duration = requireNonNull(duration, "duration is null");
    }

    @JsonProperty("duration")
    public Duration getAirliftDuration()
    {
        return duration;
    }

    @Override
    public java.time.Duration getDuration()
    {
        return java.time.Duration.ofNanos(duration.roundTo(NANOSECONDS));
    }

    @Override
    public DurationTiming mergeWith(DurationTiming other)
    {
        long durationNanos = duration.roundTo(NANOSECONDS);
        long otherDurationNanos = other.getAirliftDuration().roundTo(NANOSECONDS);
        return new DurationTiming(new Duration(durationNanos + otherDurationNanos, NANOSECONDS).convertToMostSuccinctTimeUnit());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DurationTiming that = (DurationTiming) o;
        return duration.equals(that.duration);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(duration);
    }

    @Override
    public String toString()
    {
        return toStringHelper("")
                .add("duration", duration)
                .toString();
    }
}
