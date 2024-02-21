/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.models;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public record DiscoveredPartitionValues(SlashEndedPath path, Map<LowerCaseString, String> values)
{
    public DiscoveredPartitionValues
    {
        requireNonNull(path, "path cannot be null");
        values = ImmutableMap.copyOf(values);
    }
}
