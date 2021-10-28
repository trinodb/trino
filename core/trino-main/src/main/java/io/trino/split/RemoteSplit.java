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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import io.trino.execution.TaskId;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.exchange.ExchangeSourceHandle;

import java.net.URI;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RemoteSplit
        implements ConnectorSplit
{
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

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            property = "@type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = DirectExchangeInput.class, name = "direct"),
            @JsonSubTypes.Type(value = ExternalExchangeInput.class, name = "external")})
    public interface ExchangeInput
    {
    }

    public static class DirectExchangeInput
            implements ExchangeInput
    {
        private final TaskId taskId;
        private final URI location;

        @JsonCreator
        public DirectExchangeInput(@JsonProperty("taskId") TaskId taskId, @JsonProperty("location") URI location)
        {
            this.taskId = requireNonNull(taskId, "taskId is null");
            this.location = requireNonNull(location, "location is null");
        }

        @JsonProperty
        public TaskId getTaskId()
        {
            return taskId;
        }

        @JsonProperty
        public URI getLocation()
        {
            return location;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("taskId", taskId)
                    .add("location", location)
                    .toString();
        }
    }

    public static class ExternalExchangeInput
            implements ExchangeInput
    {
        private final List<ExchangeSourceHandle> exchangeSourceHandles;

        @JsonCreator
        public ExternalExchangeInput(@JsonProperty("exchangeSourceHandles") List<ExchangeSourceHandle> exchangeSourceHandles)
        {
            this.exchangeSourceHandles = ImmutableList.copyOf(requireNonNull(exchangeSourceHandles, "exchangeSourceHandles is null"));
        }

        @JsonProperty
        public List<ExchangeSourceHandle> getExchangeSourceHandles()
        {
            return exchangeSourceHandles;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("exchangeSourceHandles", exchangeSourceHandles)
                    .toString();
        }
    }
}
