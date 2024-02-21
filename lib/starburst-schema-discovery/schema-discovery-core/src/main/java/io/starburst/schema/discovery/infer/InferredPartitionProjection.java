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

import com.google.common.collect.ImmutableSet;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.trino.plugin.hive.projection.ProjectionType;
import io.trino.plugin.hive.type.TypeInfo;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public record InferredPartitionProjection(
        boolean isEqualSignSeparatedPartition,
        ProjectionType projectionType)
{
    public static final InferredPartitionProjection NON_PROJECTED_PARTITION = new InferredPartitionProjection(true, ProjectionType.INJECTED);
    private static final Set<TypeInfo> INTEGER_PROJECTED_TYPES = ImmutableSet.of(HiveTypes.HIVE_BYTE, HiveTypes.HIVE_SHORT, HiveTypes.HIVE_INT);

    public InferredPartitionProjection
    {
        requireNonNull(projectionType, "projectionType is null");
    }

    // consider adding remaining ProjectionTypes, but this would require more custom configuration
    public static InferredPartitionProjection equalSignSeparatedFromHiveType(TypeInfo typeInfo)
    {
        TypeInfo hiveType = HiveTypes.adjustType(typeInfo);
        return INTEGER_PROJECTED_TYPES.contains(hiveType) ?
                new InferredPartitionProjection(true, ProjectionType.INTEGER) :
                new InferredPartitionProjection(true, ProjectionType.INJECTED);
    }

    public static InferredPartitionProjection customFromHiveType(TypeInfo typeInfo)
    {
        TypeInfo hiveType = HiveTypes.adjustType(typeInfo);
        return INTEGER_PROJECTED_TYPES.contains(hiveType) ?
                new InferredPartitionProjection(false, ProjectionType.INTEGER) :
                new InferredPartitionProjection(false, ProjectionType.ENUM);
    }
}
