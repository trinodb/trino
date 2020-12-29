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
package io.prestosql.plugin.kudu;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.plugin.kudu.schema.KuduRangePartition;
import io.prestosql.spi.connector.ConnectorPartitioningHandle;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class KuduPartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final String schema;
    private final String table;
    private final int bucketCount;
    private final List<Integer> bucketColumnIndexes;
    private final List<Type> bucketColumnTypes;
    private final Optional<List<KuduRangePartition>> rangePartitions;

    @JsonCreator
    public KuduPartitioningHandle(
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("bucketCount") int bucketCount,
            @JsonProperty("bucketColumnIndexes") List<Integer> bucketColumnIndexes,
            @JsonProperty("bucketColumnTypes") List<Type> bucketColumnTypes,
            @JsonProperty("rangePartitions") Optional<List<KuduRangePartition>> rangePartitions)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.table = table;
        this.bucketCount = bucketCount;
        this.bucketColumnIndexes = bucketColumnIndexes;
        this.bucketColumnTypes = requireNonNull(bucketColumnTypes, "bucketColumnTypes is null");
        this.rangePartitions = requireNonNull(rangePartitions, "rangePartitions is null");
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public int getBucketCount()
    {
        return bucketCount;
    }

    @JsonProperty
    public List<Type> getBucketColumnTypes()
    {
        return bucketColumnTypes;
    }

    @JsonProperty
    public List<Integer> getBucketColumnIndexes()
    {
        return bucketColumnIndexes;
    }

    @JsonProperty
    public Optional<List<KuduRangePartition>> getRangePartitions()
    {
        return rangePartitions;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schema", schema)
                .add("table", table)
                .add("bucketCount", bucketCount)
                .toString();
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
        KuduPartitioningHandle that = (KuduPartitioningHandle) o;
        if (this.rangePartitions.isPresent() != that.rangePartitions.isPresent()) {
            return false;
        }
        // Range partitions must have the same boundary, or we cannot run in grouped_execution mode.
        if (this.rangePartitions.isPresent()) {
            List<KuduRangePartition> thisRangePartitions = this.rangePartitions.get();
            List<KuduRangePartition> thatRangePartitions = that.rangePartitions.get();
            if (thisRangePartitions.size() != thatRangePartitions.size()) {
                return false;
            }
            for (int i = 0; i < thisRangePartitions.size(); i++) {
                if (!thisRangePartitions.get(i).equals(thatRangePartitions.get(i))) {
                    return false;
                }
            }
        }
        return bucketCount == that.bucketCount
                && Objects.equals(bucketColumnTypes, that.bucketColumnTypes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketCount, schema, table, bucketColumnTypes);
    }
}
