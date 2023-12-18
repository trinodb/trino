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

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Comparator.naturalOrder;
import static java.util.Objects.requireNonNull;

public record SnowflakeArrowSplit(
        long resultVersion,
        List<Chunk> chunks,
        SnowflakeSessionParameters snowflakeSessionParameters)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(SnowflakeArrowSplit.class);

    @JsonCreator
    public SnowflakeArrowSplit(
            @JsonProperty("resultVersion") long resultVersion,
            @JsonProperty("chunks") List<Chunk> chunks,
            @JsonProperty("snowflakeSessionParameters") SnowflakeSessionParameters snowflakeSessionParameters)
    {
        this.resultVersion = resultVersion;
        this.chunks = requireNonNull(chunks, "chunks are null");
        this.snowflakeSessionParameters = requireNonNull(snowflakeSessionParameters, "snowflakeSessionParameters are null");
    }

    @JsonIgnore
    public long getLargestChunkUncompressedBytes()
    {
        return chunks.stream().map(Chunk::uncompressedByteSize).max(naturalOrder()).orElseThrow();
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
                + estimatedSizeOf(chunks, Chunk::getRetainedSizeInBytes)
                + snowflakeSessionParameters.getRetainedSizeInBytes();
    }
}
