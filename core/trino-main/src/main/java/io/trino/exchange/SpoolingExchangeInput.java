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
import io.trino.spi.exchange.ExchangeSourceOutputSelector;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class SpoolingExchangeInput
        implements ExchangeInput
{
    private static final int INSTANCE_SIZE = instanceSize(SpoolingExchangeInput.class);

    private final List<ExchangeSourceHandle> exchangeSourceHandles;
    private final Optional<ExchangeSourceOutputSelector> outputSelector;

    @JsonCreator
    public SpoolingExchangeInput(
            @JsonProperty("exchangeSourceHandles") List<ExchangeSourceHandle> exchangeSourceHandles,
            @JsonProperty("outputSelector") Optional<ExchangeSourceOutputSelector> outputSelector)
    {
        this.exchangeSourceHandles = ImmutableList.copyOf(requireNonNull(exchangeSourceHandles, "exchangeSourceHandles is null"));
        this.outputSelector = requireNonNull(outputSelector, "outputSelector is null");
    }

    @JsonProperty
    public List<ExchangeSourceHandle> getExchangeSourceHandles()
    {
        return exchangeSourceHandles;
    }

    @JsonProperty
    public Optional<ExchangeSourceOutputSelector> getOutputSelector()
    {
        return outputSelector;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("exchangeSourceHandles", exchangeSourceHandles)
                .add("outputSelector", outputSelector)
                .toString();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(exchangeSourceHandles, ExchangeSourceHandle::getRetainedSizeInBytes)
                + sizeOf(outputSelector, ExchangeSourceOutputSelector::getRetainedSizeInBytes);
    }
}
