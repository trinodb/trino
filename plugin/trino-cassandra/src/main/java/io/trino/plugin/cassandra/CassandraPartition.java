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
package io.trino.plugin.cassandra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class CassandraPartition
{
    static final String UNPARTITIONED_ID = "<UNPARTITIONED>";
    public static final CassandraPartition UNPARTITIONED = new CassandraPartition();

    private final String partitionId;
    private final byte[] key;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final boolean indexedColumnPredicatePushdown;

    private CassandraPartition()
    {
        partitionId = UNPARTITIONED_ID;
        tupleDomain = TupleDomain.all();
        key = null;
        indexedColumnPredicatePushdown = false;
    }

    @JsonCreator
    public CassandraPartition(
            @JsonProperty("key") byte[] key,
            @JsonProperty("partitionId") String partitionId,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
            @JsonProperty("indexedColumnPredicatePushdown") boolean indexedColumnPredicatePushdown)
    {
        this.key = key;
        this.partitionId = partitionId;
        this.tupleDomain = tupleDomain;
        this.indexedColumnPredicatePushdown = indexedColumnPredicatePushdown;
    }

    public boolean isUnpartitioned()
    {
        return partitionId.equals(UNPARTITIONED_ID);
    }

    @JsonProperty
    public boolean isIndexedColumnPredicatePushdown()
    {
        return indexedColumnPredicatePushdown;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @JsonProperty
    public String getPartitionId()
    {
        return partitionId;
    }

    @Override
    public String toString()
    {
        return partitionId;
    }

    public ByteBuffer getKeyAsByteBuffer()
    {
        return ByteBuffer.wrap(key);
    }

    @JsonProperty
    public byte[] getKey()
    {
        return key;
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
        CassandraPartition other = (CassandraPartition) obj;
        return Objects.equals(this.partitionId, other.partitionId) &&
                Arrays.equals(this.key, other.key) &&
                Objects.equals(this.tupleDomain, other.tupleDomain) &&
                Objects.equals(this.indexedColumnPredicatePushdown, other.indexedColumnPredicatePushdown);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionId, Arrays.hashCode(key), tupleDomain, indexedColumnPredicatePushdown);
    }
}
