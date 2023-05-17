/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.parallel;

import com.fasterxml.jackson.annotation.JsonProperty;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;

/**
 * Only selected session properties that are used by {@link net.snowflake.client.core.arrow the converters}
 */
public record SnowflakeSessionParameters(
        @JsonProperty("timestampOutputFormat") String timestampOutputFormat,
        @JsonProperty("timestampNtzOutputFormat") String timestampNtzOutputFormat,
        @JsonProperty("timestampLtzOutputFormat") String timestampLtzOutputFormat,
        @JsonProperty("timestampTzOutputFormat") String timestampTzOutputFormat,
        @JsonProperty("dateOutputFormat") String dateOutputFormat,
        @JsonProperty("timeOutputFormat") String timeOutputFormat,
        @JsonProperty("timezone") String timezone,
        @JsonProperty("honorClientTZForTimestampNTZ") boolean honorClientTZForTimestampNTZ,
        @JsonProperty("binaryOutputFormat") String binaryOutputFormat)
{
    private static final int INSTANCE_SIZE = instanceSize(SnowflakeSessionParameters.class);

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(timestampOutputFormat) + estimatedSizeOf(timestampNtzOutputFormat) + estimatedSizeOf(timestampLtzOutputFormat) + estimatedSizeOf(
                timestampTzOutputFormat) + estimatedSizeOf(dateOutputFormat) + estimatedSizeOf(timeOutputFormat) + estimatedSizeOf(timezone) + estimatedSizeOf(binaryOutputFormat);
    }
}
