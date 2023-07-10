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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.util.StdConverter;
import io.airlift.slice.Slice;
import io.airlift.stats.TDigest;
import io.trino.spi.metrics.Distribution;

import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.base.MoreObjects.ToStringHelper;
import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.String.format;

public class TDigestHistogram
        implements Distribution<TDigestHistogram>
{
    @JsonSerialize(converter = TDigestToBase64Converter.class)
    @JsonDeserialize(converter = Base64ToTDigestConverter.class)
    private final TDigest digest;

    public static TDigestHistogram fromValue(double value)
    {
        return fromValue(value, 1);
    }

    public static TDigestHistogram fromValue(double value, double weight)
    {
        TDigest digest = new TDigest();
        digest.add(value, weight);
        return new TDigestHistogram(digest);
    }

    @JsonCreator
    public TDigestHistogram(TDigest digest)
    {
        this.digest = digest;
    }

    @JsonProperty
    public synchronized TDigest getDigest()
    {
        return TDigest.copyOf(digest);
    }

    @Override
    public TDigestHistogram mergeWith(TDigestHistogram other)
    {
        TDigest result = getDigest();
        other.mergeTo(result);
        return new TDigestHistogram(result);
    }

    @Override
    public TDigestHistogram mergeWith(List<TDigestHistogram> others)
    {
        if (others.isEmpty()) {
            return this;
        }

        TDigest result = getDigest();
        for (TDigestHistogram other : others) {
            other.mergeTo(result);
        }
        return new TDigestHistogram(result);
    }

    private synchronized void mergeTo(TDigest digest)
    {
        digest.mergeWith(this.digest);
    }

    @Override
    @JsonProperty
    public synchronized long getTotal()
    {
        return (long) digest.getCount();
    }

    // Below are extra properties that make it easy to read and parse serialized distribution
    // in operator summaries and event listener.
    @JsonProperty
    public synchronized double getMin()
    {
        return digest.getMin();
    }

    @JsonProperty
    public synchronized double getMax()
    {
        return digest.getMax();
    }

    @JsonProperty
    public synchronized double getP01()
    {
        return digest.valueAt(0.01);
    }

    @JsonProperty
    public synchronized double getP05()
    {
        return digest.valueAt(0.05);
    }

    @JsonProperty
    public synchronized double getP10()
    {
        return digest.valueAt(0.10);
    }

    @JsonProperty
    public synchronized double getP25()
    {
        return digest.valueAt(0.25);
    }

    @JsonProperty
    public synchronized double getP50()
    {
        return digest.valueAt(0.50);
    }

    @JsonProperty
    public synchronized double getP75()
    {
        return digest.valueAt(0.75);
    }

    @JsonProperty
    public synchronized double getP90()
    {
        return digest.valueAt(0.90);
    }

    @JsonProperty
    public synchronized double getP95()
    {
        return digest.valueAt(0.95);
    }

    @JsonProperty
    public synchronized double getP99()
    {
        return digest.valueAt(0.99);
    }

    @Override
    public synchronized double getPercentile(double percentile)
    {
        return digest.valueAt(percentile / 100.0);
    }

    @Override
    public String toString()
    {
        ToStringHelper helper = toStringHelper("")
                .add("count", getTotal())
                .add("p01", formatDouble(getP01()))
                .add("p05", formatDouble(getP05()))
                .add("p10", formatDouble(getP10()))
                .add("p25", formatDouble(getP25()))
                .add("p50", formatDouble(getP50()))
                .add("p75", formatDouble(getP75()))
                .add("p90", formatDouble(getP90()))
                .add("p95", formatDouble(getP95()))
                .add("p99", formatDouble(getP99()))
                .add("min", formatDouble(getMin()))
                .add("max", formatDouble(getMax()));
        return helper.toString();
    }

    public static Optional<TDigestHistogram> merge(List<TDigestHistogram> histograms)
    {
        if (histograms.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(histograms.get(0).mergeWith(histograms.subList(1, histograms.size())));
    }

    private static String formatDouble(double value)
    {
        return format(Locale.US, "%.2f", value);
    }

    public static class TDigestToBase64Converter
            extends StdConverter<TDigest, String>
    {
        public TDigestToBase64Converter()
        {
        }

        @Override
        public String convert(TDigest value)
        {
            Slice slice = value.serialize();
            return Base64.getEncoder().encodeToString(slice.getBytes());
        }
    }

    public static class Base64ToTDigestConverter
            extends StdConverter<String, TDigest>
    {
        public Base64ToTDigestConverter()
        {
        }

        @Override
        public TDigest convert(String value)
        {
            Slice slice = wrappedBuffer(Base64.getDecoder().decode(value));
            return TDigest.deserialize(slice);
        }
    }
}
