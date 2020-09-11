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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.util.HiveBucketing.BucketingVersion;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HiveBucketHandle
{
    private final List<HiveColumnHandle> columns;
    private final BucketingVersion bucketingVersion;
    // Number of buckets in the table, as specified in table metadata
    private final int tableBucketCount;
    // Number of buckets the table will appear to have when the Hive connector
    // presents the table to the engine for read.
    private final int readBucketCount;

    @JsonCreator
    public HiveBucketHandle(
            @JsonProperty("columns") List<HiveColumnHandle> columns,
            @JsonProperty("bucketingVersion") BucketingVersion bucketingVersion,
            @JsonProperty("tableBucketCount") int tableBucketCount,
            @JsonProperty("readBucketCount") int readBucketCount)
    {
        this.columns = requireNonNull(columns, "columns is null");
        columns.forEach(column -> checkArgument(column.isBaseColumn(), format("projected column %s is not allowed for bucketing", column)));
        this.bucketingVersion = requireNonNull(bucketingVersion, "bucketingVersion is null");
        this.tableBucketCount = tableBucketCount;
        this.readBucketCount = readBucketCount;
    }

    @JsonProperty
    public List<HiveColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public BucketingVersion getBucketingVersion()
    {
        return bucketingVersion;
    }

    @JsonProperty
    public int getTableBucketCount()
    {
        return tableBucketCount;
    }

    @JsonProperty
    public int getReadBucketCount()
    {
        return readBucketCount;
    }

    public HiveBucketProperty toTableBucketProperty()
    {
        return new HiveBucketProperty(
                columns.stream()
                        .map(HiveColumnHandle::getName)
                        .collect(toList()),
                bucketingVersion,
                tableBucketCount,
                ImmutableList.of());
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
        HiveBucketHandle other = (HiveBucketHandle) obj;
        return Objects.equals(this.columns, other.columns) &&
                this.bucketingVersion == other.bucketingVersion &&
                this.tableBucketCount == other.tableBucketCount &&
                this.readBucketCount == other.readBucketCount;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columns, bucketingVersion, tableBucketCount, readBucketCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columns", columns)
                .add("bucketingVersion", bucketingVersion)
                .add("tableBucketCount", tableBucketCount)
                .add("readBucketCount", readBucketCount)
                .toString();
    }
}
