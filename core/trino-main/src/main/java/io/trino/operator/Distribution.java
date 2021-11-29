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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.airlift.stats.TDigest;
import io.trino.plugin.base.metrics.TDigestHistogram;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import static java.util.Objects.requireNonNull;

@Immutable
public class Distribution
{
    @Nullable
    @JsonSerialize(converter = TDigestHistogram.TDigestToBase64Converter.class)
    @JsonDeserialize(converter = TDigestHistogram.Base64ToTDigestConverter.class)
    private final TDigest digest;
    private final double count;
    private final double min;
    private final double max;
    private final double p01;
    private final double p05;
    private final double p10;
    private final double p25;
    private final double p50;
    private final double p75;
    private final double p90;
    private final double p95;
    private final double p99;

    public static Distribution ofCount(long count)
    {
        TDigest digest = new TDigest();
        digest.add(count);
        return new Distribution(digest);
    }

    @JsonCreator
    public Distribution(
            @Nullable TDigest digest,
            double count,
            double min,
            double max,
            double p01,
            double p05,
            double p10,
            double p25,
            double p50,
            double p75,
            double p90,
            double p95,
            double p99)
    {
        this.digest = digest;
        this.count = count;
        this.min = min;
        this.max = max;
        this.p01 = p01;
        this.p05 = p05;
        this.p10 = p10;
        this.p25 = p25;
        this.p50 = p50;
        this.p75 = p75;
        this.p90 = p90;
        this.p95 = p95;
        this.p99 = p99;
    }

    private Distribution(TDigest digest)
    {
        this.digest = requireNonNull(digest, "digest is null");
        this.count = digest.getCount();
        this.min = digest.getMin();
        this.max = digest.getMax();
        this.p01 = digest.valueAt(0.01);
        this.p05 = digest.valueAt(0.05);
        this.p10 = digest.valueAt(0.10);
        this.p25 = digest.valueAt(0.25);
        this.p50 = digest.valueAt(0.5);
        this.p75 = digest.valueAt(0.75);
        this.p90 = digest.valueAt(0.90);
        this.p95 = digest.valueAt(0.95);
        this.p99 = digest.valueAt(0.99);
    }

    public Distribution mergeWith(Distribution distribution)
    {
        requireNonNull(digest, "digest is null");
        requireNonNull(distribution.getDigest(), "distribution.getDigest() is null");

        TDigest result = TDigest.copyOf(digest);
        result.mergeWith(distribution.getDigest());
        return new Distribution(result);
    }

    public Distribution pruneDigest()
    {
        if (digest == null) {
            return this;
        }
        return new Distribution(
                null,
                count,
                min,
                max,
                p01,
                p05,
                p10,
                p25,
                p50,
                p75,
                p90,
                p95,
                p99);
    }

    @Nullable
    @JsonProperty
    public TDigest getDigest()
    {
        if (digest == null) {
            return null;
        }

        return TDigest.copyOf(digest);
    }

    @JsonProperty
    public synchronized double getCount()
    {
        return count;
    }

    @JsonProperty
    public synchronized double getMin()
    {
        return min;
    }

    @JsonProperty
    public synchronized double getMax()
    {
        return max;
    }

    @JsonProperty
    public synchronized double getP01()
    {
        return p01;
    }

    @JsonProperty
    public synchronized double getP05()
    {
        return p05;
    }

    @JsonProperty
    public synchronized double getP10()
    {
        return p10;
    }

    @JsonProperty
    public synchronized double getP25()
    {
        return p25;
    }

    @JsonProperty
    public synchronized double getP50()
    {
        return p50;
    }

    @JsonProperty
    public synchronized double getP75()
    {
        return p75;
    }

    @JsonProperty
    public synchronized double getP90()
    {
        return p90;
    }

    @JsonProperty
    public synchronized double getP95()
    {
        return p95;
    }

    @JsonProperty
    public synchronized double getP99()
    {
        return p99;
    }
}
