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
package io.trino.split;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.exchange.ExchangeInput;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class RemoteSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(RemoteSplit.class);

    private final ExchangeInput exchangeInput;

    @JsonCreator
    public RemoteSplit(@JsonProperty("exchangeInput") ExchangeInput exchangeInput)
    {
        this.exchangeInput = requireNonNull(exchangeInput, "remoteSplitInput is null");
    }

    @JsonProperty
    public ExchangeInput getExchangeInput()
    {
        return exchangeInput;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("exchangeInput", exchangeInput)
                .toString();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + exchangeInput.getRetainedSizeInBytes();
    }
}
