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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.plugin.hive.util.HiveUtil.getDeserializerClassName;
import static java.util.Objects.requireNonNull;

public class HiveSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HiveSplit.class).instanceSize();

    private final String path;
    private final long start;
    private final long length;
    private final long estimatedFileSize;
    private final long fileModifiedTime;
    private final Properties schema;
    private final List<HivePartitionKey> partitionKeys;
    private final List<HostAddress> addresses;
    private final String database;
    private final String table;
    private final String partitionName;
    private final OptionalInt readBucketNumber;
    private final OptionalInt tableBucketNumber;
    private final int statementId;
    private final boolean forceLocalScheduling;
    private final TableToPartitionMapping tableToPartitionMapping;
    private final Optional<BucketConversion> bucketConversion;
    private final Optional<BucketValidation> bucketValidation;
    private final boolean s3SelectPushdownEnabled;
    private final Optional<AcidInfo> acidInfo;
    private final long splitNumber;
    private final SplitWeight splitWeight;

    @JsonCreator
    public HiveSplit(
            @JsonProperty("database") String database,
            @JsonProperty("table") String table,
            @JsonProperty("partitionName") String partitionName,
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("estimatedFileSize") long estimatedFileSize,
            @JsonProperty("fileModifiedTime") long fileModifiedTime,
            @JsonProperty("schema") Properties schema,
            @JsonProperty("partitionKeys") List<HivePartitionKey> partitionKeys,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("readBucketNumber") OptionalInt readBucketNumber,
            @JsonProperty("tableBucketNumber") OptionalInt tableBucketNumber,
            @JsonProperty("statementId") int statementId,
            @JsonProperty("forceLocalScheduling") boolean forceLocalScheduling,
            @JsonProperty("tableToPartitionMapping") TableToPartitionMapping tableToPartitionMapping,
            @JsonProperty("bucketConversion") Optional<BucketConversion> bucketConversion,
            @JsonProperty("bucketValidation") Optional<BucketValidation> bucketValidation,
            @JsonProperty("s3SelectPushdownEnabled") boolean s3SelectPushdownEnabled,
            @JsonProperty("acidInfo") Optional<AcidInfo> acidInfo,
            @JsonProperty("splitNumber") long splitNumber,
            @JsonProperty("splitWeight") SplitWeight splitWeight)
    {
        checkArgument(start >= 0, "start must be positive");
        checkArgument(length >= 0, "length must be positive");
        checkArgument(estimatedFileSize >= 0, "estimatedFileSize must be positive");
        requireNonNull(database, "database is null");
        requireNonNull(table, "table is null");
        requireNonNull(partitionName, "partitionName is null");
        requireNonNull(path, "path is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(partitionKeys, "partitionKeys is null");
        requireNonNull(addresses, "addresses is null");
        requireNonNull(readBucketNumber, "readBucketNumber is null");
        requireNonNull(tableBucketNumber, "tableBucketNumber is null");
        requireNonNull(tableToPartitionMapping, "tableToPartitionMapping is null");
        requireNonNull(bucketConversion, "bucketConversion is null");
        requireNonNull(bucketValidation, "bucketValidation is null");
        requireNonNull(acidInfo, "acidInfo is null");

        this.database = database;
        this.table = table;
        this.partitionName = partitionName;
        this.path = path;
        this.start = start;
        this.length = length;
        this.estimatedFileSize = estimatedFileSize;
        this.fileModifiedTime = fileModifiedTime;
        this.schema = schema;
        this.partitionKeys = ImmutableList.copyOf(partitionKeys);
        this.addresses = ImmutableList.copyOf(addresses);
        this.readBucketNumber = readBucketNumber;
        this.tableBucketNumber = tableBucketNumber;
        this.statementId = statementId;
        this.forceLocalScheduling = forceLocalScheduling;
        this.tableToPartitionMapping = tableToPartitionMapping;
        this.bucketConversion = bucketConversion;
        this.bucketValidation = bucketValidation;
        this.s3SelectPushdownEnabled = s3SelectPushdownEnabled;
        this.acidInfo = acidInfo;
        this.splitNumber = splitNumber;
        this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
    }

    @JsonProperty
    public String getDatabase()
    {
        return database;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public String getPartitionName()
    {
        return partitionName;
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
    public Properties getSchema()
    {
        return schema;
    }

    @JsonProperty
    public List<HivePartitionKey> getPartitionKeys()
    {
        return partitionKeys;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
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
    public int getStatementId()
    {
        return statementId;
    }

    @JsonProperty
    public boolean isForceLocalScheduling()
    {
        return forceLocalScheduling;
    }

    @JsonProperty
    public TableToPartitionMapping getTableToPartitionMapping()
    {
        return tableToPartitionMapping;
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

    @Override
    public boolean isRemotelyAccessible()
    {
        return !forceLocalScheduling;
    }

    @JsonProperty
    public boolean isS3SelectPushdownEnabled()
    {
        return s3SelectPushdownEnabled;
    }

    @JsonProperty
    public Optional<AcidInfo> getAcidInfo()
    {
        return acidInfo;
    }

    @JsonProperty
    public long getSplitNumber()
    {
        return splitNumber;
    }

    @JsonProperty
    @Override
    public SplitWeight getSplitWeight()
    {
        return splitWeight;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + estimatedSizeOf(schema, key -> estimatedSizeOf((String) key), value -> estimatedSizeOf((String) value))
                + estimatedSizeOf(partitionKeys, HivePartitionKey::getEstimatedSizeInBytes)
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes)
                + estimatedSizeOf(database)
                + estimatedSizeOf(table)
                + estimatedSizeOf(partitionName)
                + sizeOf(readBucketNumber)
                + sizeOf(tableBucketNumber)
                + tableToPartitionMapping.getEstimatedSizeInBytes()
                + sizeOf(bucketConversion, BucketConversion::getRetainedSizeInBytes)
                + sizeOf(bucketValidation, BucketValidation::getRetainedSizeInBytes)
                + sizeOf(acidInfo, AcidInfo::getRetainedSizeInBytes)
                + splitWeight.getRetainedSizeInBytes();
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("path", path)
                .put("start", start)
                .put("length", length)
                .put("estimatedFileSize", estimatedFileSize)
                .put("hosts", addresses)
                .put("database", database)
                .put("table", table)
                .put("forceLocalScheduling", forceLocalScheduling)
                .put("partitionName", partitionName)
                .put("deserializerClassName", getDeserializerClassName(schema))
                .put("s3SelectPushdownEnabled", s3SelectPushdownEnabled)
                .put("splitNumber", splitNumber)
                .buildOrThrow();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(path)
                .addValue(start)
                .addValue(length)
                .addValue(estimatedFileSize)
                .toString();
    }

    public static class BucketConversion
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(BucketConversion.class).instanceSize();

        private final BucketingVersion bucketingVersion;
        private final int tableBucketCount;
        private final int partitionBucketCount;
        private final List<HiveColumnHandle> bucketColumnNames;
        // tableBucketNumber is needed, but can be found in tableBucketNumber field of HiveSplit.

        @JsonCreator
        public BucketConversion(
                @JsonProperty("bucketingVersion") BucketingVersion bucketingVersion,
                @JsonProperty("tableBucketCount") int tableBucketCount,
                @JsonProperty("partitionBucketCount") int partitionBucketCount,
                @JsonProperty("bucketColumnHandles") List<HiveColumnHandle> bucketColumnHandles)
        {
            this.bucketingVersion = requireNonNull(bucketingVersion, "bucketingVersion is null");
            this.tableBucketCount = tableBucketCount;
            this.partitionBucketCount = partitionBucketCount;
            this.bucketColumnNames = requireNonNull(bucketColumnHandles, "bucketColumnHandles is null");
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
        public int getPartitionBucketCount()
        {
            return partitionBucketCount;
        }

        @JsonProperty
        public List<HiveColumnHandle> getBucketColumnHandles()
        {
            return bucketColumnNames;
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
            BucketConversion that = (BucketConversion) o;
            return tableBucketCount == that.tableBucketCount &&
                    partitionBucketCount == that.partitionBucketCount &&
                    Objects.equals(bucketColumnNames, that.bucketColumnNames);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableBucketCount, partitionBucketCount, bucketColumnNames);
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE
                    + estimatedSizeOf(bucketColumnNames, HiveColumnHandle::getRetainedSizeInBytes);
        }
    }

    public static class BucketValidation
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(BucketValidation.class).instanceSize();

        private final BucketingVersion bucketingVersion;
        private final int bucketCount;
        private final List<HiveColumnHandle> bucketColumns;

        @JsonCreator
        public BucketValidation(
                @JsonProperty("bucketingVersion") BucketingVersion bucketingVersion,
                @JsonProperty("bucketCount") int bucketCount,
                @JsonProperty("bucketColumns") List<HiveColumnHandle> bucketColumns)
        {
            this.bucketingVersion = requireNonNull(bucketingVersion, "bucketingVersion is null");
            this.bucketCount = bucketCount;
            this.bucketColumns = ImmutableList.copyOf(requireNonNull(bucketColumns, "bucketColumns is null"));
        }

        @JsonProperty
        public BucketingVersion getBucketingVersion()
        {
            return bucketingVersion;
        }

        @JsonProperty
        public int getBucketCount()
        {
            return bucketCount;
        }

        @JsonProperty
        public List<HiveColumnHandle> getBucketColumns()
        {
            return bucketColumns;
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE
                    + estimatedSizeOf(bucketColumns, HiveColumnHandle::getRetainedSizeInBytes);
        }
    }
}
