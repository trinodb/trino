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
import io.airlift.slice.SizeOf;

import java.util.Base64;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public record Chunk(
        Optional<String> fileUrl,
        Optional<String> encodedArrowValue,
        int uncompressedByteSize,
        int compressedByteSize,
        Map<String, String> headers)
{
    private static final int INSTANCE_SIZE = instanceSize(Chunk.class);

    static Chunk newFileChunk(String fileUrl, int uncompressedByteSize, int compressedByteSize, Map<String, String> headers)
    {
        verify(!headers.isEmpty(), "Expected security headers for remote file chunks");
        return new Chunk(Optional.of(fileUrl), Optional.empty(), uncompressedByteSize, compressedByteSize, headers);
    }

    static Chunk newInlineChunk(String encodedArrowValue)
    {
        int compressedBytes = encodedArrowValue.getBytes(UTF_8).length;
        // for inline chunk this value is not present in the metadata, so we approximate
        int uncompressedBytes = compressedBytes * 4;
        return new Chunk(Optional.empty(), Optional.of(encodedArrowValue), uncompressedBytes, compressedBytes, emptyMap());
    }

    @JsonCreator
    public Chunk(
            @JsonProperty("fileUrl") Optional<String> fileUrl,
            @JsonProperty("encodedArrowValue") Optional<String> encodedArrowValue,
            @JsonProperty("uncompressedByteSize") int uncompressedByteSize,
            @JsonProperty("compressedByteSize") int compressedByteSize,
            @JsonProperty("headers") Map<String, String> headers)
    {
        this.fileUrl = requireNonNull(fileUrl, "fileUrl is null");
        this.encodedArrowValue = requireNonNull(encodedArrowValue, "encodedArrowValue is null");
        this.uncompressedByteSize = uncompressedByteSize;
        this.compressedByteSize = compressedByteSize;
        this.headers = requireNonNull(headers, "headers is null");
    }

    @JsonIgnore
    public byte[] getInputStream(StarburstResultStreamProvider streamProvider)
    {
        if (fileUrl.isPresent()) {
            return streamProvider.getInputStream(this);
        }
        return decodeInlineData();
    }

    @JsonIgnore
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + sizeOf(fileUrl, SizeOf::estimatedSizeOf)
                + sizeOf(encodedArrowValue, SizeOf::estimatedSizeOf)
                + estimatedSizeOf(headers, SizeOf::estimatedSizeOf, SizeOf::estimatedSizeOf);
    }

    private byte[] decodeInlineData()
    {
        return Base64.getDecoder().decode(encodedArrowValue
                .orElseThrow(() -> new IllegalStateException("Either fileUrl or encodedArrowValue must be present in the split, but both are null!")));
    }
}
