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
package io.prestosql.transaction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public final class TransactionId
{
    private final UUID uuid;
    private final String nodeId;

    private TransactionId(UUID uuid, String nodeId)
    {
        this.uuid = requireNonNull(uuid, "uuid is null");
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
    }

    public static TransactionId create(String nodeId)
    {
        return new TransactionId(UUID.randomUUID(), nodeId);
    }

    String getNodeId()
    {
        return nodeId;
    }

    @JsonCreator
    public static TransactionId valueOf(String value)
    {
        List<String> parts = Splitter.on(":").limit(2).splitToList(value);
        Preconditions.checkArgument(parts.size() == 2, "Invalid transaction id: " + value);
        return new TransactionId(UUID.fromString(parts.get(0)), parts.get(1));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(uuid);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final TransactionId other = (TransactionId) obj;
        return Objects.equals(this.uuid, other.uuid);
    }

    @Override
    @JsonValue
    public String toString()
    {
        return uuid + ":" + nodeId;
    }
}
