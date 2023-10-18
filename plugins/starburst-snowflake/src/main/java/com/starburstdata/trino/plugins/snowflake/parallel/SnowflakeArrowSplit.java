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
import io.airlift.slice.SizeOf;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public record SnowflakeArrowSplit(
        Optional<String> fileUrl,
        Optional<String> encodedArrowValue,
        int uncompressedByteSize,
        int compressedByteSize,
        long resultVersion,
        Map<String, String> headers,
        SnowflakeSessionParameters snowflakeSessionParameters
)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(SnowflakeArrowSplit.class);

    // SSE-C algorithm header
    private static final String SSE_C_ALGORITHM = "x-amz-server-side-encryption-customer-algorithm";
    // SSE-C customer key header
    private static final String SSE_C_KEY = "x-amz-server-side-encryption-customer-key";
    // SSE-C algorithm value
    private static final String SSE_C_AES = "AES256";

    @JsonCreator
    public SnowflakeArrowSplit(
            @JsonProperty("fileUrl") Optional<String> fileUrl,
            @JsonProperty("encodedArrowValue") Optional<String> encodedArrowValue,
            @JsonProperty("uncompressedByteSize") int uncompressedByteSize,
            @JsonProperty("compressedByteSize") int compressedByteSize,
            @JsonProperty("resultVersion") long resultVersion,
            @JsonProperty("headers") Map<String, String> headers,
            @JsonProperty("snowflakeSessionParameters") SnowflakeSessionParameters snowflakeSessionParameters)
    {
        this.fileUrl = requireNonNull(fileUrl, "fileUrl are null");
        this.encodedArrowValue = requireNonNull(encodedArrowValue, "encodedArrowValue are null");
        this.uncompressedByteSize = uncompressedByteSize;
        this.compressedByteSize = compressedByteSize;
        this.resultVersion = resultVersion;
        this.headers = requireNonNull(headers, "headers are null");
        this.snowflakeSessionParameters = snowflakeSessionParameters;
    }

    public static SnowflakeArrowSplit newEncodedSplit(
            String encodedArrowValue,
            SnowflakeSessionParameters snowflakeSessionParameters,
            long resultVersion)
    {
        return new SnowflakeArrowSplit(Optional.empty(), Optional.of(encodedArrowValue), 0, 0, resultVersion, emptyMap(), snowflakeSessionParameters);
    }

    public static SnowflakeArrowSplit newChunkFileSplit(
            String fileUrl,
            int uncompressedByteSize,
            int compressedByteSize,
            Map<String, String> chunkHeaders,
            SnowflakeSessionParameters snowflakeSessionParameters,
            String queryMasterKey,
            long resultVersion)
    {
        if (!chunkHeaders.isEmpty()) {
            return new SnowflakeArrowSplit(Optional.of(fileUrl), Optional.empty(), uncompressedByteSize, compressedByteSize,  resultVersion, chunkHeaders, snowflakeSessionParameters);
        }
        else if (queryMasterKey != null) {
            return new SnowflakeArrowSplit(
                    Optional.of(fileUrl),
                    null,
                    uncompressedByteSize,
                    compressedByteSize,
                    resultVersion,
                    buildMasterKeyAuthHeaders(queryMasterKey),
                    snowflakeSessionParameters);
        }
        throw new IllegalStateException("Security headers or query master key must be present if there is a chunk URL");
    }

    @JsonIgnore
    public byte[] getInputStream(StarburstResultStreamProvider streamProvider)
    {
        if (fileUrl.isPresent()) {
            return streamProvider.getInputStream(this);
        }
        return decodeInlineData();
    }

    private byte[] decodeInlineData()
    {
        return Base64.getDecoder().decode(encodedArrowValue
                        .orElseThrow(() -> new IllegalStateException("Either fileUrl or encodedArrowValue must be present in the split, but both are null!")));
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
                + sizeOf(fileUrl, SizeOf::estimatedSizeOf)
                + sizeOf(encodedArrowValue, SizeOf::estimatedSizeOf)
                + estimatedSizeOf(headers, SizeOf::estimatedSizeOf, SizeOf::estimatedSizeOf)
                + snowflakeSessionParameters.getRetainedSizeInBytes();
    }

    private static Map<String, String> buildMasterKeyAuthHeaders(String queryMasterKey)
    {
        Map<String, String> headers = new HashMap<>();
        headers.put(SSE_C_ALGORITHM, SSE_C_AES);
        headers.put(SSE_C_KEY, queryMasterKey);
        return headers;
    }
}
