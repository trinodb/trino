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
package io.trino.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.Objects;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class HiveTransactionHandle
        implements ConnectorTransactionHandle
{
    private final boolean autoCommit;
    private final UUID uuid;

    public HiveTransactionHandle(boolean autoCommit)
    {
        this(autoCommit, UUID.randomUUID());
    }

    @JsonCreator
    public HiveTransactionHandle(@JsonProperty("autoCommit") boolean autoCommit, @JsonProperty("uuid") UUID uuid)
    {
        this.autoCommit = autoCommit;
        this.uuid = requireNonNull(uuid, "uuid is null");
    }

    @JsonProperty
    public boolean isAutoCommit()
    {
        return autoCommit;
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
        HiveTransactionHandle that = (HiveTransactionHandle) o;
        return autoCommit == that.autoCommit && Objects.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(autoCommit, uuid);
    }

    @Override
    public String toString()
    {
        return uuid.toString();
    }
}
