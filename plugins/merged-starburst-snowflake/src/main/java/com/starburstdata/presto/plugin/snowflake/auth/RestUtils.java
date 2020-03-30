/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.auth;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import io.airlift.log.Logger;
import io.prestosql.spi.security.AccessDeniedException;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import okhttp3.Call;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;

public class RestUtils<T>
{
    private static final Logger log = Logger.get(RestUtils.class);

    private RestUtils()
    {
    }

    public static <T> T jsonPath(DocumentContext json, String path)
    {
        try {
            return json.read(path);
        }
        catch (RuntimeException e) {
            log.error(e, "Failed to find JSON path [%s] in: %s", path, json.jsonString());
            throw e;
        }
    }

    public static DocumentContext parseJsonResponse(Call call)
            throws IOException
    {
        Response response = executeWithRetries(call);
        if (response.code() != 200) {
            switch (response.code()) {
                // Snowflake tends to return 400 for authn/authz issues
                case 400:
                case 401:
                case 403:
                    if (response.body() != null) {
                        try (ResponseBody body = response.body()) {
                            log.info("HTTP response code %s; response body was %s", response.code(), body.string());
                        }
                    }
                    else {
                        log.info("HTTP response code %s", response.code());
                    }
                    throw new AccessDeniedException("OAuth token request call failed for: " + call.request());
            }
        }
        return parseJsonResponse(response);
    }

    public static DocumentContext parseJsonResponse(Response response)
            throws IOException
    {
        try (InputStream in = response.body().byteStream()) {
            return JsonPath.parse(in);
        }
    }

    public static Response executeWithRetries(Call call)
    {
        // we cannot execute a call more than once, so clone in case we need to retry
        return Failsafe.with(retryPolicy()).get(() -> requireNonNull(call, "call is null").clone().execute());
    }

    public static <T> RetryPolicy<T> retryPolicy()
    {
        return new RetryPolicy<T>()
                .withBackoff(1, 5, SECONDS)
                .withMaxDuration(Duration.of(30, SECONDS));
    }
}
