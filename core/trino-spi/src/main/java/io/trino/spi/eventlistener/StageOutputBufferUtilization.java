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
package io.trino.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.Unstable;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * This class is JSON serializable for convenience and serialization compatibility is not guaranteed across versions.
 */
public class StageOutputBufferUtilization
{
    private final int stageId;
    private final int tasks;
    private final double p01;
    private final double p05;
    private final double p10;
    private final double p25;
    private final double p50;
    private final double p75;
    private final double p90;
    private final double p95;
    private final double p99;
    private final double min;
    private final double max;
    private final Duration duration;

    @JsonCreator
    @Unstable
    public StageOutputBufferUtilization(
            int stageId,
            int tasks,
            double p01,
            double p05,
            double p10,
            double p25,
            double p50,
            double p75,
            double p90,
            double p95,
            double p99,
            double min,
            double max,
            Duration duration)
    {
        this.stageId = stageId;
        this.tasks = tasks;
        this.p01 = p01;
        this.p05 = p05;
        this.p10 = p10;
        this.p25 = p25;
        this.p50 = p50;
        this.p75 = p75;
        this.p90 = p90;
        this.p95 = p95;
        this.p99 = p99;
        this.min = min;
        this.max = max;
        this.duration = requireNonNull(duration, "duration is null");
    }

    @JsonProperty
    public int getStageId()
    {
        return stageId;
    }

    @JsonProperty
    public int getTasks()
    {
        return tasks;
    }

    @JsonProperty
    public double getP01()
    {
        return p01;
    }

    @JsonProperty
    public double getP05()
    {
        return p05;
    }

    @JsonProperty
    public double getP10()
    {
        return p10;
    }

    @JsonProperty
    public double getP25()
    {
        return p25;
    }

    @JsonProperty
    public double getP50()
    {
        return p50;
    }

    @JsonProperty
    public double getP75()
    {
        return p75;
    }

    @JsonProperty
    public double getP90()
    {
        return p90;
    }

    @JsonProperty
    public double getP95()
    {
        return p95;
    }

    @JsonProperty
    public double getP99()
    {
        return p99;
    }

    @JsonProperty
    public double getMin()
    {
        return min;
    }

    @JsonProperty
    public double getMax()
    {
        return max;
    }

    @JsonProperty
    public Duration getDuration()
    {
        return duration;
    }
}
