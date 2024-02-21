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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.starburst.schema.discovery.internal.Column;

import java.util.Collection;
import java.util.List;

public record DiscoveredColumns(List<Column> columns, Collection<String> flags)
{
    public static final DiscoveredColumns EMPTY_DISCOVERED_COLUMNS = new DiscoveredColumns(ImmutableList.of(), ImmutableList.of());

    public DiscoveredColumns
    {
        columns = ImmutableList.copyOf(columns);
        flags = ImmutableSet.copyOf(flags);
    }
}
