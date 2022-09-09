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
package io.trino.execution.buffer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SpoolingOutputBuffers
        extends OutputBuffers
{
    private final ExchangeSinkInstanceHandle exchangeSinkInstanceHandle;

    public static SpoolingOutputBuffers createInitial(ExchangeSinkInstanceHandle exchangeSinkInstanceHandle)
    {
        return new SpoolingOutputBuffers(0, exchangeSinkInstanceHandle);
    }

    @JsonCreator
    public SpoolingOutputBuffers(
            @JsonProperty("version") long version,
            @JsonProperty("exchangeSinkInstanceHandle") ExchangeSinkInstanceHandle exchangeSinkInstanceHandle)
    {
        super(version);
        this.exchangeSinkInstanceHandle = requireNonNull(exchangeSinkInstanceHandle, "exchangeSinkInstanceHandle is null");
    }

    @JsonProperty
    public ExchangeSinkInstanceHandle getExchangeSinkInstanceHandle()
    {
        return exchangeSinkInstanceHandle;
    }

    @Override
    public void checkValidTransition(OutputBuffers outputBuffers)
    {
        requireNonNull(outputBuffers, "outputBuffers is null");
        checkArgument(outputBuffers instanceof SpoolingOutputBuffers, "outputBuffers is expected to be an instance of SpoolingOutputBuffers");
        if (getVersion() > outputBuffers.getVersion()) {
            throw new IllegalArgumentException("new outputBuffers version is older");
        }
    }
}
