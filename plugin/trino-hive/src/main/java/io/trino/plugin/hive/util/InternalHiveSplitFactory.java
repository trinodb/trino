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
package io.trino.plugin.hive.util;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.HiveSplit;
import io.trino.plugin.hive.HiveSplit.BucketConversion;
import io.trino.plugin.hive.InternalHiveSplit;
import io.trino.plugin.hive.InternalHiveSplit.InternalHiveBlock;
import io.trino.plugin.hive.TableToPartitionMapping;
import io.trino.plugin.hive.fs.BlockLocation;
import io.trino.plugin.hive.fs.TrinoFileStatus;
import io.trino.plugin.hive.orc.OrcPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.trino.plugin.hive.s3select.S3SelectPushdown;
import io.trino.spi.HostAddress;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.function.BooleanSupplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveColumnHandle.isPathColumnHandle;
import static io.trino.plugin.hive.util.HiveUtil.isSplittable;
import static java.util.Objects.requireNonNull;

public class InternalHiveSplitFactory
{
    private final FileSystem fileSystem;
    private final String partitionName;
    private final InputFormat<?, ?> inputFormat;
    private final Properties strippedSchema;
    private final List<HivePartitionKey> partitionKeys;
    private final Optional<Domain> pathDomain;
    private final TableToPartitionMapping tableToPartitionMapping;
    private final BooleanSupplier partitionMatchSupplier;
    private final Optional<BucketConversion> bucketConversion;
    private final Optional<HiveSplit.BucketValidation> bucketValidation;
    private final long minimumTargetSplitSizeInBytes;
    private final Optional<Long> maxSplitFileSize;
    private final boolean forceLocalScheduling;
    private final boolean s3SelectPushdownEnabled;

    public InternalHiveSplitFactory(
            FileSystem fileSystem,
            String partitionName,
            InputFormat<?, ?> inputFormat,
            Properties schema,
            List<HivePartitionKey> partitionKeys,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            BooleanSupplier partitionMatchSupplier,
            TableToPartitionMapping tableToPartitionMapping,
            Optional<BucketConversion> bucketConversion,
            Optional<HiveSplit.BucketValidation> bucketValidation,
            DataSize minimumTargetSplitSize,
            boolean forceLocalScheduling,
            boolean s3SelectPushdownEnabled,
            Optional<Long> maxSplitFileSize)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.partitionName = requireNonNull(partitionName, "partitionName is null");
        this.inputFormat = requireNonNull(inputFormat, "inputFormat is null");
        this.strippedSchema = stripUnnecessaryProperties(requireNonNull(schema, "schema is null"));
        this.partitionKeys = requireNonNull(partitionKeys, "partitionKeys is null");
        pathDomain = getPathDomain(requireNonNull(effectivePredicate, "effectivePredicate is null"));
        this.partitionMatchSupplier = requireNonNull(partitionMatchSupplier, "partitionMatchSupplier is null");
        this.tableToPartitionMapping = requireNonNull(tableToPartitionMapping, "tableToPartitionMapping is null");
        this.bucketConversion = requireNonNull(bucketConversion, "bucketConversion is null");
        this.bucketValidation = requireNonNull(bucketValidation, "bucketValidation is null");
        this.forceLocalScheduling = forceLocalScheduling;
        this.s3SelectPushdownEnabled = s3SelectPushdownEnabled;
        this.minimumTargetSplitSizeInBytes = minimumTargetSplitSize.toBytes();
        this.maxSplitFileSize = requireNonNull(maxSplitFileSize, "maxSplitFileSize is null");
        checkArgument(minimumTargetSplitSizeInBytes > 0, "minimumTargetSplitSize must be > 0, found: %s", minimumTargetSplitSize);
    }

    private static Properties stripUnnecessaryProperties(Properties schema)
    {
        // Sending the full schema with every split is costly and can be avoided for formats supported natively
        schema = OrcPageSourceFactory.stripUnnecessaryProperties(schema);
        schema = ParquetPageSourceFactory.stripUnnecessaryProperties(schema);
        schema = RcFilePageSourceFactory.stripUnnecessaryProperties(schema);
        return schema;
    }

    public String getPartitionName()
    {
        return partitionName;
    }

    public Optional<InternalHiveSplit> createInternalHiveSplit(TrinoFileStatus status, OptionalInt readBucketNumber, OptionalInt tableBucketNumber, boolean splittable, Optional<AcidInfo> acidInfo)
    {
        splittable = splittable &&
                status.getLength() > minimumTargetSplitSizeInBytes &&
                isSplittable(inputFormat, fileSystem, new Path(status.getPath()));
        return createInternalHiveSplit(
                status.getPath(),
                status.getBlockLocations(),
                0,
                status.getLength(),
                status.getLength(),
                status.getModificationTime(),
                readBucketNumber,
                tableBucketNumber,
                splittable,
                acidInfo);
    }

    public Optional<InternalHiveSplit> createInternalHiveSplit(FileSplit split)
            throws IOException
    {
        FileStatus file = fileSystem.getFileStatus(split.getPath());
        return createInternalHiveSplit(
                split.getPath().toString(),
                BlockLocation.fromHiveBlockLocations(fileSystem.getFileBlockLocations(file, split.getStart(), split.getLength())),
                split.getStart(),
                split.getLength(),
                file.getLen(),
                file.getModificationTime(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                false,
                Optional.empty());
    }

    private Optional<InternalHiveSplit> createInternalHiveSplit(
            String path,
            List<BlockLocation> blockLocations,
            long start,
            long length,
            // Estimated because, for example, encrypted S3 files may be padded, so reported size may not reflect actual size
            long estimatedFileSize,
            long fileModificationTime,
            OptionalInt readBucketNumber,
            OptionalInt tableBucketNumber,
            boolean splittable,
            Optional<AcidInfo> acidInfo)
    {
        if (!pathMatchesPredicate(pathDomain, path)) {
            return Optional.empty();
        }

        // per HIVE-13040 empty files are allowed
        if (estimatedFileSize == 0) {
            return Optional.empty();
        }

        // Dynamic filter may not have been ready when partition was loaded in BackgroundHiveSplitLoader,
        // but it might be ready when splits are enumerated lazily.
        if (!partitionMatchSupplier.getAsBoolean()) {
            return Optional.empty();
        }

        if (maxSplitFileSize.isPresent() && estimatedFileSize > maxSplitFileSize.get()) {
            return Optional.empty();
        }

        ImmutableList.Builder<InternalHiveBlock> blockBuilder = ImmutableList.builder();
        for (BlockLocation blockLocation : blockLocations) {
            // clamp the block range
            long blockStart = Math.max(start, blockLocation.getOffset());
            long blockEnd = Math.min(start + length, blockLocation.getOffset() + blockLocation.getLength());
            if (blockStart > blockEnd) {
                // block is outside split range
                continue;
            }
            if (blockStart == blockEnd && !(blockStart == start && blockEnd == start + length)) {
                // skip zero-width block, except in the special circumstance: slice is empty, and the block covers the empty slice interval.
                continue;
            }
            blockBuilder.add(new InternalHiveBlock(blockStart, blockEnd, getHostAddresses(blockLocation)));
        }
        List<InternalHiveBlock> blocks = blockBuilder.build();
        checkBlocks(path, blocks, start, length);

        if (!splittable) {
            // not splittable, use the hosts from the first block if it exists
            blocks = ImmutableList.of(new InternalHiveBlock(start, start + length, blocks.get(0).getAddresses()));
        }

        return Optional.of(new InternalHiveSplit(
                partitionName,
                path,
                start,
                start + length,
                estimatedFileSize,
                fileModificationTime,
                strippedSchema,
                partitionKeys,
                blocks,
                readBucketNumber,
                tableBucketNumber,
                splittable,
                forceLocalScheduling && allBlocksHaveAddress(blocks),
                tableToPartitionMapping,
                bucketConversion,
                bucketValidation,
                s3SelectPushdownEnabled && S3SelectPushdown.isCompressionCodecSupported(inputFormat, path),
                acidInfo,
                partitionMatchSupplier));
    }

    private static void checkBlocks(String path, List<InternalHiveBlock> blocks, long start, long length)
    {
        checkArgument(start >= 0, "Split (%s) has negative start (%s)", path, start);
        checkArgument(length >= 0, "Split (%s) has negative length (%s)", path, length);
        checkArgument(!blocks.isEmpty(), "Split (%s) has no blocks", path);
        checkArgument(
                start == blocks.get(0).getStart(),
                "Split (%s) start (%s) does not match first block start (%s)",
                path,
                start,
                blocks.get(0).getStart());
        checkArgument(
                start + length == blocks.get(blocks.size() - 1).getEnd(),
                "Split (%s) end (%s) does not match last block end (%s)",
                path,
                start + length,
                blocks.get(blocks.size() - 1).getEnd());
        for (int i = 1; i < blocks.size(); i++) {
            checkArgument(
                    blocks.get(i - 1).getEnd() == blocks.get(i).getStart(),
                    "Split (%s) block end (%s) does not match next block start (%s)",
                    path,
                    blocks.get(i - 1).getEnd(),
                    blocks.get(i).getStart());
        }
    }

    private static boolean allBlocksHaveAddress(Collection<InternalHiveBlock> blocks)
    {
        return blocks.stream()
                .map(InternalHiveBlock::getAddresses)
                .noneMatch(List::isEmpty);
    }

    private static List<HostAddress> getHostAddresses(BlockLocation blockLocation)
    {
        // Hadoop FileSystem returns "localhost" as a default
        return blockLocation.getHosts().stream()
                .map(HostAddress::fromString)
                .filter(address -> !address.getHostText().equals("localhost"))
                .collect(toImmutableList());
    }

    private static Optional<Domain> getPathDomain(TupleDomain<HiveColumnHandle> effectivePredicate)
    {
        return effectivePredicate.getDomains()
                .flatMap(domains -> domains.entrySet().stream()
                        .filter(entry -> isPathColumnHandle(entry.getKey()))
                        .map(Map.Entry::getValue)
                        .findFirst());
    }

    private static boolean pathMatchesPredicate(Optional<Domain> pathDomain, String path)
    {
        return pathDomain
                .map(domain -> domain.includesNullableValue(utf8Slice(path)))
                .orElse(true);
    }
}
