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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import net.snowflake.client.core.SessionUtil;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.starburstdata.trino.plugins.snowflake.parallel.Chunk.newFileChunk;
import static com.starburstdata.trino.plugins.snowflake.parallel.Chunk.newInlineChunk;
import static com.starburstdata.trino.plugins.snowflake.parallel.SnowflakeParallelSessionProperties.getTargetSplitSize;
import static java.util.Collections.emptyMap;
import static net.snowflake.client.core.ResultUtil.effectiveParamValue;

final class ChunkParser
{
    // SSE-C algorithm header
    private static final String SSE_C_ALGORITHM = "x-amz-server-side-encryption-customer-algorithm";
    // SSE-C customer key header
    private static final String SSE_C_KEY = "x-amz-server-side-encryption-customer-key";
    // SSE-C algorithm value
    private static final String SSE_C_AES = "AES256";

    private ChunkParser() {}

    /**
     * Originates from {@link net.snowflake.client.jdbc.SnowflakeResultSetSerializableV1#parseChunkFiles()}
     */
    public static List<ConnectorSplit> parseChunks(ConnectorSession session, JsonNode rootNode)
    {
        JsonNode data = rootNode.path("data");
        JsonNode chunksNode = data.path("chunks");

        // try to get the Query Result Master Key
        JsonNode qrmkNode = data.path("qrmk");
        String qrmk = qrmkNode.isMissingNode() ? null : qrmkNode.textValue();

        // parse chunk headers
        Map<String, String> chunkHeadersMap = new HashMap<>();
        JsonNode chunkHeaders = data.path("chunkHeaders");
        if (chunkHeaders != null && !chunkHeaders.isMissingNode()) {
            Iterator<Entry<String, JsonNode>> chunkHeadersIter = chunkHeaders.fields();
            while (chunkHeadersIter.hasNext()) {
                Entry<String, JsonNode> chunkHeader = chunkHeadersIter.next();
                chunkHeadersMap.put(chunkHeader.getKey(), chunkHeader.getValue().asText());
            }
        }

        SnowflakeSessionParameters parameters = parseParameters(SessionUtil.getCommonParams(data.path("parameters")));
        long resultVersion = !data.path("version").isMissingNode() ? data.path("version").longValue() : 0;

        // we will encounter both chunks and rowset value at the same time, or just the rowset value for small queries
        JsonNode rowsetBase64 = data.path("rowsetBase64");
        boolean encodedChunkIsPresentInJson = !rowsetBase64.isMissingNode() && !rowsetBase64.asText("").isBlank();

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        ImmutableList.Builder<Chunk> chunks = ImmutableList.builder();

        long limitInBytes = getTargetSplitSize(session).toBytes();
        int currentBatchSize = 0;

        if (encodedChunkIsPresentInJson) {
            Chunk inlineChunk = newInlineChunk(rowsetBase64.asText());
            chunks.add(inlineChunk);
            currentBatchSize += inlineChunk.compressedByteSize();
        }

        Map<String, String> headers = chunksNode.isEmpty() ? emptyMap() : buildSecurityHeaders(chunkHeadersMap, qrmk);
        // parse chunk files metadata, e.g. url
        for (int index = 0; index < chunksNode.size(); index++) {
            JsonNode chunkNode = chunksNode.get(index);
            String url = chunkNode.path("url").asText();
            int compressedSize = chunkNode.path("compressedSize").asInt();
            int uncompressedSize = chunkNode.path("uncompressedSize").asInt();
            Chunk chunk = newFileChunk(url, uncompressedSize, compressedSize, headers);

            if (currentBatchSize + chunk.compressedByteSize() > limitInBytes) {
                splits.add(new SnowflakeArrowSplit(resultVersion, chunks.build(), parameters));
                chunks = ImmutableList.builder();
                currentBatchSize = 0;
            }

            chunks.add(chunk);
            currentBatchSize += chunk.compressedByteSize();
        }

        ImmutableList<Chunk> remainingChunks = chunks.build();
        if (!remainingChunks.isEmpty()) {
            splits.add(new SnowflakeArrowSplit(resultVersion, remainingChunks, parameters));
        }

        return splits.build();
    }

    private static Map<String, String> buildSecurityHeaders(Map<String, String> chunkHeaders, String queryMasterKey)
    {
        if (chunkHeaders.isEmpty() && queryMasterKey == null) {
            throw new IllegalStateException("Security headers or query master key must be present if there is a chunk URL");
        }
        return !chunkHeaders.isEmpty() ? chunkHeaders : ImmutableMap.of(
                SSE_C_ALGORITHM, SSE_C_AES,
                SSE_C_KEY, queryMasterKey);
    }

    private static SnowflakeSessionParameters parseParameters(Map<String, Object> parameters)
    {
        return new SnowflakeSessionParameters(
                (String) effectiveParamValue(parameters, "TIMESTAMP_OUTPUT_FORMAT"),
                (String) effectiveParamValue(parameters, "TIMESTAMP_NTZ_OUTPUT_FORMAT"),
                (String) effectiveParamValue(parameters, "TIMESTAMP_LTZ_OUTPUT_FORMAT"),
                (String) effectiveParamValue(parameters, "TIMESTAMP_TZ_OUTPUT_FORMAT"),
                (String) effectiveParamValue(parameters, "DATE_OUTPUT_FORMAT"),
                (String) effectiveParamValue(parameters, "TIME_OUTPUT_FORMAT"),
                (String) effectiveParamValue(parameters, "TIMEZONE"),
                (boolean) effectiveParamValue(parameters, "CLIENT_HONOR_CLIENT_TZ_FOR_TIMESTAMP_NTZ"),
                (String) effectiveParamValue(parameters, "BINARY_OUTPUT_FORMAT"));
    }
}
