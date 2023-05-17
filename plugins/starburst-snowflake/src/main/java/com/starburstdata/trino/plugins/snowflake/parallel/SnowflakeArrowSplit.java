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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class SnowflakeArrowSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(SnowflakeArrowSplit.class);

    // SSE-C algorithm header
    private static final String SSE_C_ALGORITHM = "x-amz-server-side-encryption-customer-algorithm";
    // SSE-C customer key header
    private static final String SSE_C_KEY = "x-amz-server-side-encryption-customer-key";
    // SSE-C algorithm value
    private static final String SSE_C_AES = "AES256";

    private final Optional<String> fileUrl;
    private final Optional<String> encodedArrowValue;
    private final int uncompressedByteSize;
    private final int compressedByteSize;
    private final int rowCount;
    private final long resultVersion;
    private final Map<String, String> headers;
    private final SnowflakeSessionParameters snowflakeSessionParameters;

    @JsonCreator
    public SnowflakeArrowSplit(
            @JsonProperty("fileUrl") String fileUrl,
            @JsonProperty("encodedArrowValue") String encodedArrowValue,
            @JsonProperty("uncompressedByteSize") int uncompressedByteSize,
            @JsonProperty("compressedByteSize") int compressedByteSize,
            @JsonProperty("rowCount") int rowCount,
            @JsonProperty("resultVersion") long resultVersion,
            @JsonProperty("headers") Map<String, String> headers,
            @JsonProperty("snowflakeSessionParameters") SnowflakeSessionParameters snowflakeSessionParameters)
    {
        this.fileUrl = Optional.ofNullable(fileUrl);
        this.encodedArrowValue = Optional.ofNullable(encodedArrowValue);
        this.uncompressedByteSize = uncompressedByteSize;
        this.compressedByteSize = compressedByteSize;
        this.rowCount = rowCount;
        this.resultVersion = resultVersion;
        this.headers = requireNonNull(headers, "headers are null");
        this.snowflakeSessionParameters = snowflakeSessionParameters;
    }

    public static SnowflakeArrowSplit newEncodedSplit(
            String encodedArrowValue,
            SnowflakeSessionParameters snowflakeSessionParameters,
            long resultVersion)
    {
        return new SnowflakeArrowSplit(null, encodedArrowValue, 0, 0, 0, resultVersion, Collections.emptyMap(), snowflakeSessionParameters);
    }

    public static SnowflakeArrowSplit newChunkFileSplit(
            String fileUrl,
            int uncompressedByteSize,
            int compressedByteSize,
            int rowCount,
            Map<String, String> chunkHeaders,
            SnowflakeSessionParameters snowflakeSessionParameters,
            String queryMasterKey,
            long resultVersion)
    {
        if (!chunkHeaders.isEmpty()) {
            return new SnowflakeArrowSplit(fileUrl, null, uncompressedByteSize, compressedByteSize, rowCount, resultVersion, chunkHeaders, snowflakeSessionParameters);
        }
        else if (queryMasterKey != null) {
            return new SnowflakeArrowSplit(
                    fileUrl,
                    null,
                    uncompressedByteSize,
                    compressedByteSize,
                    rowCount,
                    resultVersion,
                    buildMasterKeyAuthHeaders(queryMasterKey),
                    snowflakeSessionParameters);
        }
        throw new IllegalStateException("Security headers or query master key must be present if there is a chunk URL");
    }

    public InputStream getInputStream(StarburstResultStreamProvider starburstResultStreamProvider)
    {
        return fileUrl.map(url -> starburstResultStreamProvider.getInputStream(this))
                .orElseGet(() -> new ByteArrayInputStream(
                        Base64.getDecoder().decode(encodedArrowValue
                                .orElseThrow(() -> new IllegalStateException("Either fileUrl or encodedArrowValue must be present in the split, but both are null!")))));
    }

    @JsonProperty
    public Optional<String> getFileUrl()
    {
        return fileUrl;
    }

    @JsonProperty
    public long getResultVersion()
    {
        return resultVersion;
    }

    @JsonProperty
    public SnowflakeSessionParameters getSnowflakeSessionParameters()
    {
        return snowflakeSessionParameters;
    }

    @JsonProperty
    public Optional<String> getEncodedArrowValue()
    {
        return encodedArrowValue;
    }

    @JsonProperty
    public int getUncompressedByteSize()
    {
        return uncompressedByteSize;
    }

    @JsonProperty
    public int getCompressedByteSize()
    {
        return compressedByteSize;
    }

    @JsonProperty
    public int getRowCount()
    {
        return rowCount;
    }

    @JsonProperty
    public Map<String, String> getHeaders()
    {
        return headers;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

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
