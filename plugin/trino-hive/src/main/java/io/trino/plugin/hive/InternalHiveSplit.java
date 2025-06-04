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
import com.google.common.collect.ImmutableMap;
import io.trino.annotation.NotThreadSafe;
import io.trino.metastore.HiveTypeName;
import io.trino.plugin.hive.HiveSplit.BucketConversion;
import io.trino.plugin.hive.HiveSplit.BucketValidation;
import io.trino.spi.HostAddress;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.BooleanSupplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class InternalHiveSplit
{
    private static final int INSTANCE_SIZE = instanceSize(InternalHiveSplit.class) + instanceSize(OptionalInt.class);
    private static final int INTEGER_INSTANCE_SIZE = instanceSize(Integer.class);

    private final String path;
    private final long end;
    private final long estimatedFileSize;
    private final long fileModifiedTime;
    private final Schema schema;
    private final List<HivePartitionKey> partitionKeys;
    private final List<InternalHiveBlock> blocks;
    private final String partitionName;
    private final OptionalInt readBucketNumber;
    private final OptionalInt tableBucketNumber;
    private final boolean splittable;
    private final boolean forceLocalScheduling;
    private final Map<Integer, HiveTypeName> hiveColumnCoercions;
    private final Optional<BucketConversion> bucketConversion;
    private final Optional<BucketValidation> bucketValidation;
    private final Optional<AcidInfo> acidInfo;
    private final BooleanSupplier partitionMatchSupplier;

    private long start;
    private int currentBlockIndex;

    public InternalHiveSplit(
            String partitionName,
            String path,
            long start,
            long end,
            long estimatedFileSize,
            long fileModifiedTime,
            Schema schema,
            List<HivePartitionKey> partitionKeys,
            List<InternalHiveBlock> blocks,
            OptionalInt readBucketNumber,
            OptionalInt tableBucketNumber,
            boolean splittable,
            boolean forceLocalScheduling,
            Map<Integer, HiveTypeName> hiveColumnCoercions,
            Optional<BucketConversion> bucketConversion,
            Optional<BucketValidation> bucketValidation,
            Optional<AcidInfo> acidInfo,
            BooleanSupplier partitionMatchSupplier)
    {
        checkArgument(start >= 0, "start must be positive");
        checkArgument(end >= 0, "length must be positive");
        checkArgument(estimatedFileSize >= 0, "fileSize must be positive");
        requireNonNull(partitionName, "partitionName is null");
        requireNonNull(path, "path is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(partitionKeys, "partitionKeys is null");
        requireNonNull(blocks, "blocks is null");
        requireNonNull(readBucketNumber, "readBucketNumber is null");
        requireNonNull(tableBucketNumber, "tableBucketNumber is null");
        requireNonNull(hiveColumnCoercions, "hiveColumnCoercions is null");
        requireNonNull(bucketConversion, "bucketConversion is null");
        requireNonNull(bucketValidation, "bucketValidation is null");
        requireNonNull(acidInfo, "acidInfo is null");
        requireNonNull(partitionMatchSupplier, "partitionMatchSupplier is null");

        this.partitionName = partitionName;
        this.path = path;
        this.start = start;
        this.end = end;
        this.estimatedFileSize = estimatedFileSize;
        this.fileModifiedTime = fileModifiedTime;
        this.schema = schema;
        this.partitionKeys = ImmutableList.copyOf(partitionKeys);
        this.blocks = ImmutableList.copyOf(blocks);
        this.readBucketNumber = readBucketNumber;
        this.tableBucketNumber = tableBucketNumber;
        this.splittable = splittable;
        this.forceLocalScheduling = forceLocalScheduling;
        this.hiveColumnCoercions = ImmutableMap.copyOf(hiveColumnCoercions);
        this.bucketConversion = bucketConversion;
        this.bucketValidation = bucketValidation;
        this.acidInfo = acidInfo;
        this.partitionMatchSupplier = partitionMatchSupplier;
    }

    public String getPath()
    {
        return path;
    }

    public long getStart()
    {
        return start;
    }

    public long getEnd()
    {
        return end;
    }

    public long getEstimatedFileSize()
    {
        return estimatedFileSize;
    }

    public long getFileModifiedTime()
    {
        return fileModifiedTime;
    }

    public Schema getSchema()
    {
        return schema;
    }

    public List<HivePartitionKey> getPartitionKeys()
    {
        return partitionKeys;
    }

    public String getPartitionName()
    {
        return partitionName;
    }

    public OptionalInt getReadBucketNumber()
    {
        return readBucketNumber;
    }

    public OptionalInt getTableBucketNumber()
    {
        return tableBucketNumber;
    }

    public boolean isSplittable()
    {
        return splittable;
    }

    public boolean isForceLocalScheduling()
    {
        return forceLocalScheduling;
    }

    public Map<Integer, HiveTypeName> getHiveColumnCoercions()
    {
        return hiveColumnCoercions;
    }

    public Optional<BucketConversion> getBucketConversion()
    {
        return bucketConversion;
    }

    public Optional<BucketValidation> getBucketValidation()
    {
        return bucketValidation;
    }

    public InternalHiveBlock currentBlock()
    {
        checkState(!isDone(), "All blocks have been consumed");
        return blocks.get(currentBlockIndex);
    }

    public boolean isDone()
    {
        return currentBlockIndex == blocks.size();
    }

    public void increaseStart(long value)
    {
        start += value;
        if (start == currentBlock().getEnd()) {
            currentBlockIndex++;
            if (isDone()) {
                return;
            }
            verify(start == currentBlock().getStart());
        }
    }

    public int getEstimatedSizeInBytes()
    {
        long result = INSTANCE_SIZE +
                estimatedSizeOf(path) +
                estimatedSizeOf(partitionKeys, HivePartitionKey::estimatedSizeInBytes) +
                estimatedSizeOf(blocks, InternalHiveBlock::getEstimatedSizeInBytes) +
                estimatedSizeOf(partitionName) +
                estimatedSizeOf(hiveColumnCoercions, (Integer key) -> INTEGER_INSTANCE_SIZE, HiveTypeName::getEstimatedSizeInBytes);
        return toIntExact(result);
    }

    public Optional<AcidInfo> getAcidInfo()
    {
        return acidInfo;
    }

    public BooleanSupplier getPartitionMatchSupplier()
    {
        return partitionMatchSupplier;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("path", path)
                .add("start", start)
                .add("end", end)
                .add("estimatedFileSize", estimatedFileSize)
                .toString();
    }

    public static class InternalHiveBlock
    {
        private static final int INSTANCE_SIZE = instanceSize(InternalHiveBlock.class);
        private static final int HOST_ADDRESS_INSTANCE_SIZE = instanceSize(HostAddress.class);

        private final long start;
        private final long end;
        private final List<HostAddress> addresses;

        public InternalHiveBlock(long start, long end, List<HostAddress> addresses)
        {
            checkArgument(start <= end, "block end cannot be before block start");
            this.start = start;
            this.end = end;
            this.addresses = ImmutableList.copyOf(addresses);
        }

        public long getStart()
        {
            return start;
        }

        public long getEnd()
        {
            return end;
        }

        public List<HostAddress> getAddresses()
        {
            return addresses;
        }

        public long getEstimatedSizeInBytes()
        {
            return INSTANCE_SIZE + estimatedSizeOf(addresses, address -> HOST_ADDRESS_INSTANCE_SIZE + estimatedSizeOf(address.getHostText()));
        }
    }
}
