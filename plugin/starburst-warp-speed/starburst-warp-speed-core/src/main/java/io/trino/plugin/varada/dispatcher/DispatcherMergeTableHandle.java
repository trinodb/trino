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
package io.trino.plugin.varada.dispatcher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorMergeTableHandle;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class DispatcherMergeTableHandle
        implements ConnectorMergeTableHandle
{
    private final DispatcherTableHandle dispatcherTableHandle;
    private final ConnectorMergeTableHandle proxyConnectorMergeTableHandle;

    @JsonCreator
    public DispatcherMergeTableHandle(
            @JsonProperty("dispatcherTableHandle") DispatcherTableHandle dispatcherTableHandle,
            @JsonProperty("proxyConnectorMergeTableHandle") ConnectorMergeTableHandle proxyConnectorMergeTableHandle)
    {
        this.dispatcherTableHandle = requireNonNull(dispatcherTableHandle);
        this.proxyConnectorMergeTableHandle = requireNonNull(proxyConnectorMergeTableHandle);
    }

    @Override
    @JsonProperty("dispatcherTableHandle")
    public DispatcherTableHandle getTableHandle()
    {
        return dispatcherTableHandle;
    }

    @JsonProperty("proxyConnectorMergeTableHandle")
    public ConnectorMergeTableHandle getProxyConnectorMergeTableHandle()
    {
        return proxyConnectorMergeTableHandle;
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
        DispatcherMergeTableHandle that = (DispatcherMergeTableHandle) o;
        return Objects.equals(dispatcherTableHandle, that.dispatcherTableHandle) && Objects.equals(proxyConnectorMergeTableHandle, that.proxyConnectorMergeTableHandle);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(dispatcherTableHandle, proxyConnectorMergeTableHandle);
    }

    @Override
    public String toString()
    {
        return "DispatcherMergeTableHandle{" +
                "dispatcherTableHandle=" + dispatcherTableHandle +
                ", proxyConnectorMergeTableHandle=" + proxyConnectorMergeTableHandle +
                '}';
    }
}
