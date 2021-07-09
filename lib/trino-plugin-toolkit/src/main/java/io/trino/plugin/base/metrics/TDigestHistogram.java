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

import static io.airlift.slice.Slices.wrappedBuffer;

public class TDigestHistogram
        implements Distribution<TDigestHistogram>
{
    @JsonSerialize(converter = TDigestToBase64Converter.class)
    @JsonDeserialize(converter = Base64ToTDigestConverter.class)
    private final TDigest digest;

    @JsonCreator
    public TDigestHistogram(TDigest digest)
    {
        this.digest = digest;
    }

    @JsonProperty
    public TDigest getDigest()
    {
        return digest;
    }

    @Override
    public TDigestHistogram mergeWith(TDigestHistogram other)
    {
        TDigest result = TDigest.copyOf(digest);
        result.mergeWith(other.getDigest());
        return new TDigestHistogram(result);
    }

    @Override
    public long getTotal()
    {
        return (long) digest.getCount();
    }

    @Override
    public double getPercentile(double percentile)
    {
        return digest.valueAt(percentile / 100.0);
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
