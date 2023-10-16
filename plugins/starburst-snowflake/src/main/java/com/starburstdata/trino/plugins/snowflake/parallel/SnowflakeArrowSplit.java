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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public record SnowflakeArrowSplit(
        long resultVersion,
        Chunk chunk,
        SnowflakeSessionParameters snowflakeSessionParameters)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(SnowflakeArrowSplit.class);

    @JsonCreator
    public SnowflakeArrowSplit(
            @JsonProperty("resultVersion") long resultVersion,
            @JsonProperty("chunk") Chunk chunk,
            @JsonProperty("snowflakeSessionParameters") SnowflakeSessionParameters snowflakeSessionParameters)
    {
        this.resultVersion = resultVersion;
        this.chunk = requireNonNull(chunk, "chunk is null");
        this.snowflakeSessionParameters = requireNonNull(snowflakeSessionParameters, "snowflakeSessionParameters are null");
    }

    @JsonIgnore
    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @JsonIgnore
    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @JsonIgnore
    @Override
    public Object getInfo()
    {
        return this;
    }

    @JsonIgnore
    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + chunk.getRetainedSizeInBytes()
                + snowflakeSessionParameters.getRetainedSizeInBytes();
    }

    @JsonIgnore
    public int uncompressedByteSize()
    {
        return chunk.uncompressedByteSize();
    }
}
