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
import com.google.errorprone.annotations.DoNotCall;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public record SingleValueHistogram(double value)
        implements TDigestHistogram
{
    public SingleValueHistogram {
        checkArgument(!Double.isNaN(value), "value is NaN");
        checkArgument(!Double.isInfinite(value), "value must be finite");
    }

    @JsonProperty(DIGEST_PROPERTY)
    public byte[] serialize()
    {
        Slice slice = Slices.allocate(Double.SIZE);
        slice.setDouble(0, value);
        return slice.getBytes();
    }

    @JsonCreator
    @DoNotCall
    public static SingleValueHistogram deserialize(@JsonProperty(DIGEST_PROPERTY) byte[] digest)
    {
        Slice slice = Slices.wrappedBuffer(digest);
        return new SingleValueHistogram(slice.getDouble(0));
    }

    @Override
    @JsonProperty
    public long getTotal()
    {
        return 1;
    }

    @Override
    @JsonProperty
    public double getMin()
    {
        return value;
    }

    @Override
    @JsonProperty
    public double getMax()
    {
        return value;
    }

    @Override
    public double[] getPercentiles(double... percentiles)
    {
        double[] values = new double[percentiles.length];
        for (int i = 0; i < percentiles.length; i++) {
            values[i] = value;
        }
        return values;
    }

    @Override
    public String toString()
    {
        return toStringHelper("")
                .add("value", value())
                .toString();
    }
}
