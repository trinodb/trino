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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * @param tableBucketCount Number of buckets in the table, as specified in table metadata
 * @param readBucketCount Number of buckets the table will appear to have when the Hive connector presents the table to the engine for read.
 */
public record HiveBucketHandle(
        List<HiveColumnHandle> columns,
        BucketingVersion bucketingVersion,
        int tableBucketCount,
        int readBucketCount,
        List<SortingColumn> sortedBy)
{
    public HiveBucketHandle
    {
        columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        columns.forEach(column -> checkArgument(column.isBaseColumn(), format("projected column %s is not allowed for bucketing", column)));
        requireNonNull(bucketingVersion, "bucketingVersion is null");
        sortedBy = ImmutableList.copyOf(requireNonNull(sortedBy, "sortedBy is null"));
    }

    public HiveBucketProperty toTableBucketProperty()
    {
        return new HiveBucketProperty(
                columns.stream()
                        .map(HiveColumnHandle::getName)
                        .collect(toList()),
                tableBucketCount,
                sortedBy);
    }
}
