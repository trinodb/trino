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
package io.trino.plugin.ducklake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.UUID;

/**
 * Transaction handle for Ducklake connector.
 * Each transaction maps to a potential new snapshot in the Ducklake catalog.
 */
public class DucklakeTransactionHandle
        implements ConnectorTransactionHandle
{
    private final UUID uuid;

    public DucklakeTransactionHandle()
    {
        this.uuid = UUID.randomUUID();
    }

    @JsonCreator
    public DucklakeTransactionHandle(@JsonProperty("uuid") UUID uuid)
    {
        this.uuid = uuid;
    }

    @JsonProperty
    public UUID getUuid()
    {
        return uuid;
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
        DucklakeTransactionHandle that = (DucklakeTransactionHandle) o;
        return uuid.equals(that.uuid);
    }

    @Override
    public int hashCode()
    {
        return uuid.hashCode();
    }

    @Override
    public String toString()
    {
        return uuid.toString();
    }
}
