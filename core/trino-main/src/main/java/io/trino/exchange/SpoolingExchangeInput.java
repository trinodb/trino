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
package io.trino.exchange;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.exchange.ExchangeSourceHandle;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.util.Objects.requireNonNull;

public class SpoolingExchangeInput
        implements ExchangeInput
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SpoolingExchangeInput.class).instanceSize();

    private final List<ExchangeSourceHandle> exchangeSourceHandles;

    @JsonCreator
    public SpoolingExchangeInput(@JsonProperty("exchangeSourceHandles") List<ExchangeSourceHandle> exchangeSourceHandles)
    {
        this.exchangeSourceHandles = ImmutableList.copyOf(requireNonNull(exchangeSourceHandles, "exchangeSourceHandles is null"));
    }

    @JsonProperty
    public List<ExchangeSourceHandle> getExchangeSourceHandles()
    {
        return exchangeSourceHandles;
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
        SpoolingExchangeInput that = (SpoolingExchangeInput) o;
        return Objects.equals(exchangeSourceHandles, that.exchangeSourceHandles);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(exchangeSourceHandles);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("exchangeSourceHandles", exchangeSourceHandles)
                .toString();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(exchangeSourceHandles, ExchangeSourceHandle::getRetainedSizeInBytes);
    }
}
