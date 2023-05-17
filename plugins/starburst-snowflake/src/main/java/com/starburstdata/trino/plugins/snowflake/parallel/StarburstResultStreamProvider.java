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

import io.trino.spi.TrinoException;
import net.snowflake.client.jdbc.RestRequest;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.internal.apache.http.Header;
import net.snowflake.client.jdbc.internal.apache.http.HttpEntity;
import net.snowflake.client.jdbc.internal.apache.http.HttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpGet;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.utils.URIBuilder;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Objects.requireNonNull;
import static net.snowflake.client.core.Constants.MB;
import static net.snowflake.client.jdbc.DefaultResultStreamProvider.detectGzipAndGetStream;

/**
 * {@link net.snowflake.client.jdbc.DefaultResultStreamProvider} adapted to work with the split
 */
public class StarburstResultStreamProvider
{
    private static final int STREAM_BUFFER_SIZE = MB;
    private static final int NETWORK_TIMEOUT_IN_MILLI = 0;
    private static final int AUTH_TIMEOUT_IN_SECONDS = 0;
    private static final int SOCKET_TIMEOUT_IN_MILLI = 0;
    private final CloseableHttpClient httpClient;

    @Inject
    public StarburstResultStreamProvider(CloseableHttpClient httpClient)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient are null");
    }

    public InputStream getInputStream(SnowflakeArrowSplit split)
    {
        HttpResponse response;
        try {
            response = getResultChunk(split);
        }
        catch (URISyntaxException | SnowflakeSQLException e) {
            throw new TrinoException(
                    JDBC_ERROR,
                    "Error encountered when request a result chunk URL: %s %s".formatted(split.getFileUrl(), firstNonNull(e.getMessage(), e)),
                    e);
        }

        InputStream inputStream;
        final HttpEntity entity = response.getEntity();
        try {
            // read the chunk data
            inputStream = detectContentEncodingAndGetInputStream(response, entity.getContent());
        }
        catch (Exception ex) {
            throw new TrinoException(
                    JDBC_ERROR,
                    "Failed to detect encoding and get the data stream: %s".formatted(response));
        }

        return inputStream;
    }

    private HttpResponse getResultChunk(SnowflakeArrowSplit split)
            throws URISyntaxException, SnowflakeSQLException
    {
        URIBuilder uriBuilder = new URIBuilder(split.getFileUrl().orElseThrow());
        HttpGet httpRequest = new HttpGet(uriBuilder.build());

        for (Map.Entry<String, String> entry : split.getHeaders().entrySet()) {
            httpRequest.addHeader(entry.getKey(), entry.getValue());
        }

        HttpResponse response =
                RestRequest.execute(
                        httpClient,
                        httpRequest,
                        NETWORK_TIMEOUT_IN_MILLI / 1000, // retry timeout
                        AUTH_TIMEOUT_IN_SECONDS,
                        SOCKET_TIMEOUT_IN_MILLI,
                        0,
                        0, // no socket timeout injection
                        null, // no canceling
                        false, // no cookie
                        false, // no retry parameters in url
                        false, // no request_guid
                        true); // retry on HTTP403 for AWS S3
        if (response == null || response.getStatusLine().getStatusCode() != 200) {
            throw new TrinoException(
                    JDBC_ERROR,
                    "Error encountered when downloading a result chunk: HTTP status=%s".formatted((response != null) ? response.getStatusLine().getStatusCode() : "null response"));
        }
        return response;
    }

    private InputStream detectContentEncodingAndGetInputStream(HttpResponse response, InputStream is)
            throws IOException
    {
        Header encoding = response.getFirstHeader("Content-Encoding");
        if (encoding != null) {
            if ("gzip".equalsIgnoreCase(encoding.getValue())) {
                // specify buffer size for GZIPInputStream
                return new GZIPInputStream(is, STREAM_BUFFER_SIZE);
            }
            throw new TrinoException(JDBC_ERROR, "Exception: unexpected compression got %s".formatted(encoding.getValue()));
        }
        return detectGzipAndGetStream(is);
    }
}
