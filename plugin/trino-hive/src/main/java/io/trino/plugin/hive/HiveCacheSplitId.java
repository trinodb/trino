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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveSplit.BucketConversion;
import io.trino.plugin.hive.HiveSplit.BucketValidation;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public class HiveCacheSplitId
{
    private final String path;
    private final long start;
    private final long length;
    private final long estimatedFileSize;
    private final long fileModifiedTime;
    private final List<HivePartitionKey> partitionKeys;
    private final String partitionName;
    private final OptionalInt readBucketNumber;
    private final OptionalInt tableBucketNumber;
    private final Map<Integer, HiveTypeName> hiveColumnCoercions;
    private final Optional<BucketConversion> bucketConversion;
    private final Optional<BucketValidation> bucketValidation;
    private final Map<String, String> schema;

    public HiveCacheSplitId(
            String path,
            long start,
            long length,
            long estimatedFileSize,
            long fileModifiedTime,
            List<HivePartitionKey> partitionKeys,
            String partitionName,
            OptionalInt readBucketNumber,
            OptionalInt tableBucketNumber,
            Map<Integer, HiveTypeName> hiveColumnCoercions,
            Optional<BucketConversion> bucketConversion,
            Optional<BucketValidation> bucketValidation,
            Map<String, String> schema)
    {
        this.path = requireNonNull(path, "path is null");
        this.start = start;
        this.length = length;
        this.estimatedFileSize = estimatedFileSize;
        this.fileModifiedTime = fileModifiedTime;
        this.partitionKeys = requireNonNull(partitionKeys, "partitionKeys is null");
        this.partitionName = requireNonNull(partitionName, "partitionName is null");
        this.readBucketNumber = requireNonNull(readBucketNumber, "readBucketNumber is null");
        this.tableBucketNumber = requireNonNull(tableBucketNumber, "tableBucketNumber is null");
        this.hiveColumnCoercions = ImmutableMap.copyOf(requireNonNull(hiveColumnCoercions, "hiveColumnCoercions is null"));
        this.bucketConversion = requireNonNull(bucketConversion, "bucketConversion is null");
        this.bucketValidation = requireNonNull(bucketValidation, "bucketValidation is null");
        this.schema = requireNonNull(schema, "schema is null");
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getLength()
    {
        return length;
    }

    @JsonProperty
    public long getEstimatedFileSize()
    {
        return estimatedFileSize;
    }

    @JsonProperty
    public long getFileModifiedTime()
    {
        return fileModifiedTime;
    }

    @JsonProperty
    public List<HivePartitionKey> getPartitionKeys()
    {
        return partitionKeys;
    }

    @JsonProperty
    public String getPartitionName()
    {
        return partitionName;
    }

    @JsonProperty
    public OptionalInt getReadBucketNumber()
    {
        return readBucketNumber;
    }

    @JsonProperty
    public OptionalInt getTableBucketNumber()
    {
        return tableBucketNumber;
    }

    @JsonProperty
    public Map<Integer, HiveTypeName> getHiveColumnCoercions()
    {
        return hiveColumnCoercions;
    }

    @JsonProperty
    public Optional<BucketConversion> getBucketConversion()
    {
        return bucketConversion;
    }

    @JsonProperty
    public Optional<BucketValidation> getBucketValidation()
    {
        return bucketValidation;
    }

    @JsonProperty
    public Map<String, String> getSchema()
    {
        return schema;
    }
}
