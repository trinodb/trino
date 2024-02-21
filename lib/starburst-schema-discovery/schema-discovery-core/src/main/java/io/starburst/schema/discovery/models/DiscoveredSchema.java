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

import java.util.List;

import static java.util.Objects.requireNonNull;

public record DiscoveredSchema(SlashEndedPath rootPath, List<DiscoveredTable> tables, List<String> errors)
{
    public DiscoveredSchema
    {
        requireNonNull(rootPath, "rootPath cannot be null");
        tables = ImmutableList.copyOf(tables);
        errors = ImmutableList.copyOf(errors);
    }
}
