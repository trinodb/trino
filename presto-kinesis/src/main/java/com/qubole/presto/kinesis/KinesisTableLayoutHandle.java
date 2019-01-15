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
package com.qubole.presto.kinesis;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Table layout handle introduced more recently in the API.
 * <p>
 * Created by derekbennett on 6/17/16.
 */
public class KinesisTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final String connectorId;
    private final KinesisTableHandle tableHandle;

    @JsonCreator
    public KinesisTableLayoutHandle(@JsonProperty("connectorId") String connectorId,
            @JsonProperty("table") KinesisTableHandle table)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.tableHandle = table;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public KinesisTableHandle getTable()
    {
        return tableHandle;
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
        KinesisTableLayoutHandle that = (KinesisTableLayoutHandle) o;
        return Objects.equals(tableHandle, that.tableHandle);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableHandle);
    }

    @Override
    public String toString()
    {
        return tableHandle.toString();
    }
}
