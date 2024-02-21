/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.infer;

import com.google.common.collect.ImmutableList;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.internal.HiveType;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.starburst.schema.discovery.models.LowerCaseString;
import io.starburst.schema.discovery.options.DiscoveryMode;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.trino.filesystem.Location;
import io.trino.plugin.hive.type.TypeInfo;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.starburst.schema.discovery.infer.InferredPartitionProjection.customFromHiveType;
import static io.starburst.schema.discovery.infer.InferredPartitionProjection.equalSignSeparatedFromHiveType;
import static io.starburst.schema.discovery.internal.HiveTypes.STRING_TYPE;
import static io.starburst.schema.discovery.io.LocationUtils.directoryOrFileName;
import static io.starburst.schema.discovery.io.LocationUtils.parentOf;
import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;

public class InferPartitions
{
    private final List<InferredPartition> partitions;
    private final LowerCaseString tableName;
    private final Location path;

    public static final String PARTITION_SEPARATOR = "=";
    public static final String PROJECTED_PARTITION_NAME = "partition_";

    public InferPartitions(GeneralOptions options, Location root, Location path)
    {
        AtomicInteger partitionProjectionCounter = new AtomicInteger();
        boolean lookForBuckets = options.lookForBuckets();
        Location partitionPath = path;
        ImmutableList.Builder<InferredPartition> builder = ImmutableList.<InferredPartition>builder();
        InferType inferType = new InferType(options, ImmutableList.of());
        while (path != null) {
            boolean isRoot = path.equals(root);
            if (isRoot) {
                break;
            }
            boolean parentIsRoot = parentOf(path).path().equals("/") || parentOf(path).equals(root);
            String pathName = directoryOrFileName(path);
            int equalsIndex = pathName.indexOf(PARTITION_SEPARATOR);
            if (equalsIndex < 0) {
                if (parentIsRoot) {
                    break;
                }
                if (options.discoveryMode() == DiscoveryMode.NORMAL && options.discoverPartitionProjection()) {
                    TypeInfo typeInfo = inferType.inferType(NullType.NULL_TYPE, pathName);
                    Column partitionColumn = new Column(toLowerCase(PROJECTED_PARTITION_NAME + partitionProjectionCounter.incrementAndGet()), new HiveType(HiveTypes.adjustType(typeInfo)));
                    builder.add(new InferredPartition(partitionPath, partitionColumn, pathName, false, customFromHiveType(typeInfo)));

                    path = parentOf(path);
                    continue;
                }
                if (!lookForBuckets) {
                    break;
                }
            }

            TypeInfo partitionType;
            String partitionName;
            String value;
            if ((equalsIndex >= 0) && ((equalsIndex + 1) < pathName.length())) {
                partitionName = pathName.substring(0, equalsIndex);
                value = pathName.substring(equalsIndex + 1);
                partitionType = inferType.inferType(NullType.NULL_TYPE, value);
            }
            else {
                partitionName = pathName;
                partitionType = STRING_TYPE;
                value = "";
            }
            Column partitionColumn = new Column(toLowerCase(partitionName), new HiveType(HiveTypes.adjustType(partitionType)));
            builder.add(new InferredPartition(partitionPath, partitionColumn, value, value.isEmpty(), equalSignSeparatedFromHiveType(partitionType)));
            path = parentOf(path);
        }
        tableName = (path != null) ? toLowerCase(directoryOrFileName(path)) : toLowerCase("-");
        this.path = (path != null) ? path : root;
        partitions = builder.build().reverse();
    }

    public static boolean isPartitionName(String name)
    {
        return name.contains(PARTITION_SEPARATOR);
    }

    public List<InferredPartition> partitions()
    {
        return partitions;
    }

    public LowerCaseString tableName()
    {
        return tableName;
    }

    public Location path()
    {
        return path;
    }
}
