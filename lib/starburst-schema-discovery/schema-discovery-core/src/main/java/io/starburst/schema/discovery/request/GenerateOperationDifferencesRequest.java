/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.request;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.models.DiscoveredTable;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public record GenerateOperationDifferencesRequest(URI uri, List<DiscoveredTable> oldTables, List<DiscoveredTable> updatedTables, Map<String, String> options, GenerateOptions generateOptions)
{
    public GenerateOperationDifferencesRequest
    {
        requireNonNull(uri, "uri cannot be null");
        requireNonNull(generateOptions, "generateOptions cannot be null");
        oldTables = ImmutableList.copyOf(oldTables);
        updatedTables = ImmutableList.copyOf(updatedTables);
        options = ImmutableMap.copyOf(options);
    }
}
