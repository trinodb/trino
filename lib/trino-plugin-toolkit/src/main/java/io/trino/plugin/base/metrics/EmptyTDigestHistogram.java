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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class EmptyTDigestHistogram
        implements TDigestHistogram
{
    @Override
    public long getTotal()
    {
        return 0;
    }

    @Override
    public double getMin()
    {
        return Double.NaN;
    }

    @Override
    public double getMax()
    {
        return Double.NaN;
    }

    @Override
    public double[] getPercentiles(double... percentiles)
    {
        double[] values = new double[percentiles.length];
        for (int i = 0; i < percentiles.length; i++) {
            values[i] = Double.NaN;
        }
        return values;
    }

    @JsonProperty(DIGEST_PROPERTY)
    public byte[] serialize()
    {
        return "empty".getBytes(UTF_8);
    }

    @JsonCreator
    @DoNotCall
    public static EmptyTDigestHistogram deserialize(@JsonProperty(DIGEST_PROPERTY) byte[] ignored)
    {
        return new EmptyTDigestHistogram();
    }

    @Override
    public String toString()
    {
        return toStringHelper("")
                .add("value", Double.NaN)
                .toString();
    }
}
