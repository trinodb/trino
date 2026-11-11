/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.UnexpectedResponseException;
import io.trino.client.TrinoJsonCodec;

import java.io.IOException;
import java.util.Set;

import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.ResponseHandlerUtils.getResponseBytes;
import static io.airlift.http.client.ResponseHandlerUtils.isJsonUtf8Content;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Parses responses with the Jackson 2 based {@link TrinoJsonCodec} used by the Trino clients,
 * since the server itself uses Jackson 3.
 */
public class TrinoJsonResponseHandler<T>
        implements ResponseHandler<T, RuntimeException>
{
    public static <T> TrinoJsonResponseHandler<T> createTrinoJsonResponseHandler(TrinoJsonCodec<T> jsonCodec)
    {
        return new TrinoJsonResponseHandler<>(jsonCodec);
    }

    public static <T> ResponseHandler<TrinoJsonResponse<T>, RuntimeException> createFullTrinoJsonResponseHandler(TrinoJsonCodec<T> jsonCodec)
    {
        return new ResponseHandler<>()
        {
            @Override
            public TrinoJsonResponse<T> handleException(Request request, Exception exception)
            {
                throw propagate(request, exception);
            }

            @Override
            public TrinoJsonResponse<T> handle(Request request, Response response)
            {
                byte[] bytes = getResponseBytes(request, response);
                if (response.getHeader(CONTENT_TYPE).isEmpty() || !isJsonUtf8Content(response)) {
                    return new TrinoJsonResponse<>(response.getStatusCode(), response.getHeaders(), null);
                }
                return new TrinoJsonResponse<>(response.getStatusCode(), response.getHeaders(), parse(jsonCodec, bytes));
            }
        };
    }

    public record TrinoJsonResponse<T>(int statusCode, ListMultimap<HeaderName, String> headers, T value)
    {
        public TrinoJsonResponse
        {
            headers = ImmutableListMultimap.copyOf(headers);
        }

        public String getHeader(HeaderName name)
        {
            return headers.get(name).stream().findFirst().orElse(null);
        }
    }

    private static final Set<Integer> SUCCESSFUL_RESPONSE_CODES = ImmutableSet.of(200, 201, 202, 203, 204, 205, 206);

    private final TrinoJsonCodec<T> jsonCodec;

    private TrinoJsonResponseHandler(TrinoJsonCodec<T> jsonCodec)
    {
        this.jsonCodec = jsonCodec;
    }

    @Override
    public T handleException(Request request, Exception exception)
    {
        throw propagate(request, exception);
    }

    @Override
    public T handle(Request request, Response response)
    {
        if (!SUCCESSFUL_RESPONSE_CODES.contains(response.getStatusCode())) {
            throw new UnexpectedResponseException(
                    String.format("Expected response code to be %s, but was %d", SUCCESSFUL_RESPONSE_CODES, response.getStatusCode()),
                    request,
                    response);
        }

        if (response.getHeader(CONTENT_TYPE).isEmpty()) {
            throw new UnexpectedResponseException("Content-Type is not set for response", request, response);
        }
        if (!isJsonUtf8Content(response)) {
            throw new UnexpectedResponseException("Expected application/json response from server but got " + response.getHeader(CONTENT_TYPE).orElse(null), request, response);
        }

        return parse(jsonCodec, getResponseBytes(request, response));
    }

    private static <T> T parse(TrinoJsonCodec<T> jsonCodec, byte[] bytes)
    {
        try {
            return jsonCodec.fromJson(new String(bytes, UTF_8));
        }
        catch (IOException e) {
            String json = new String(bytes, UTF_8);
            throw new IllegalArgumentException(String.format("Unable to create %s from JSON response: <%s>", jsonCodec.getType(), json), e);
        }
    }
}
