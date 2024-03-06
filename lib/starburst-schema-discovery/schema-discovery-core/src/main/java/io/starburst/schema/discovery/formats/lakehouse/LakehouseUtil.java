/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.formats.lakehouse;

import io.starburst.schema.discovery.io.DiscoveryTrinoFileSystem;
import io.starburst.schema.discovery.models.TableFormat;
import io.starburst.schema.discovery.models.TablePath;
import io.starburst.schema.discovery.processor.Processor.ProcessorPath;
import io.trino.filesystem.Location;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.schema.discovery.infer.InferPartitions.isPartitionName;
import static io.starburst.schema.discovery.io.LocationUtils.directoryOrFileName;
import static io.starburst.schema.discovery.io.LocationUtils.parentOf;
import static io.starburst.schema.discovery.models.TablePath.asTablePath;
import static io.trino.plugin.iceberg.IcebergUtil.METADATA_FOLDER_NAME;
import static io.trino.plugin.iceberg.IcebergUtil.getLatestMetadataLocation;
import static java.util.stream.Collectors.toMap;

public class LakehouseUtil
{
    private LakehouseUtil() {}

    public static TablePath enhanceIcebergTableLocationFromMetadata(DiscoveryTrinoFileSystem fileSystem, TablePath rootTablePath)
    {
        String latestMetadataLocation = getLatestMetadataLocation(fileSystem, rootTablePath.path());
        TableMetadata tableMetadata = TableMetadataParser.read(new ForwardingFileIo(fileSystem), latestMetadataLocation);
        return asTablePath(tableMetadata.location());
    }

    public static Optional<LakehouseFormat> checkIcebergFormatMatch(Location path)
    {
        Location parent = parentOf(path);
        String name = directoryOrFileName(parent);
        if (isPartitionName(name)) {
            parent = parentOf(parent);
        }
        if ("data".equals(name) || METADATA_FOLDER_NAME.equals(name)) {
            return Optional.of(new LakehouseFormat(TableFormat.ICEBERG, parent));
        }
        return Optional.empty();
    }

    public static List<ProcessorPath> applyDeltaLakeFormatMatch(Location root, List<ProcessorPath> processorPaths)
    {
        Map<Location, ProcessorPath> deltaLakePaths = processorPaths.stream()
                .flatMap(processorPath -> {
                    Optional<Location> maybeDeltaLakePath = deltaLakeParent(root, parentOf(processorPath.path()));
                    return maybeDeltaLakePath.stream().map(path -> new ProcessorPath(path, Optional.of(new LakehouseFormat(TableFormat.DELTA_LAKE, path))));
                })
                .collect(toMap(ProcessorPath::path, Function.identity(), (p1, ignore) -> p1));  // ignore duplicates

        Stream<ProcessorPath> nonDeltaPaths = processorPaths.stream().filter(processorPath -> isNotInDeltaLakeSet(deltaLakePaths.keySet(), root, processorPath.path()));

        return Stream.concat(deltaLakePaths.values().stream(), nonDeltaPaths).collect(toImmutableList());
    }

    public static Optional<Location> deltaLakeParent(Location root, Location parent)
    {
        while (parent != null && !parent.equals(root)) {
            if (directoryOrFileName(parent).equals("_delta_log")) {
                return Optional.of(parent);
            }
            parent = parentOf(parent);
        }
        return Optional.empty();
    }

    private static boolean isNotInDeltaLakeSet(Set<Location> deltaLakePaths, Location root, Location path)
    {
        while (path != null && !path.equals(root)) {
            if (deltaLakePaths.contains(path)) {
                return false;
            }
            path = parentOf(path);
        }
        return true;
    }
}
