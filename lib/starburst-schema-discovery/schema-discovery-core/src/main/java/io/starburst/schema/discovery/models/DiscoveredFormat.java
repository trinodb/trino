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
import io.starburst.schema.discovery.formats.csv.CsvOptions;

import java.util.Map;

import static io.starburst.schema.discovery.models.TableFormat.ERROR;
import static java.util.Objects.requireNonNull;

public record DiscoveredFormat(TableFormat format, Map<String, String> options)
{
    public static final DiscoveredFormat EMPTY_DISCOVERED_FORMAT = new DiscoveredFormat(ERROR, CsvOptions.standard());

    public DiscoveredFormat
    {
        requireNonNull(format, "format is null");
        options = ImmutableMap.copyOf(options);
    }
}
