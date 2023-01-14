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
package io.trino.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.net.HttpHeaders.LOCATION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class JsonResponse<T>
{
    private final int statusCode;
    private final Headers headers;
    @Nullable
    private final String responseBody;
    private final boolean hasValue;
    private final T value;
    private final IllegalArgumentException exception;

    private JsonResponse(int statusCode, Headers headers, String responseBody)
    {
        this.statusCode = statusCode;
        this.headers = requireNonNull(headers, "headers is null");
        this.responseBody = requireNonNull(responseBody, "responseBody is null");

        this.hasValue = false;
        this.value = null;
        this.exception = null;
    }

    private JsonResponse(int statusCode, Headers headers, @Nullable String responseBody, @Nullable T value, @Nullable IllegalArgumentException exception)
    {
        this.statusCode = statusCode;
        this.headers = requireNonNull(headers, "headers is null");
        this.responseBody = responseBody;
        this.value = value;
        this.exception = exception;
        this.hasValue = (exception == null);
    }

    public int getStatusCode()
    {
        return statusCode;
    }

    public Headers getHeaders()
    {
        return headers;
    }

    public boolean hasValue()
    {
        return hasValue;
    }

    public T getValue()
    {
        if (!hasValue) {
            throw new IllegalStateException("Response does not contain a JSON value", exception);
        }
        return value;
    }

    public Optional<String> getResponseBody()
    {
        return Optional.ofNullable(responseBody);
    }

    @Nullable
    public IllegalArgumentException getException()
    {
        return exception;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("statusCode", statusCode)
                .add("headers", headers.toMultimap())
                .add("hasValue", hasValue)
                .add("value", value)
                .omitNullValues()
                .toString();
    }

    public static <T> JsonResponse<T> execute(JsonCodec<T> codec, OkHttpClient client, Request request, OptionalLong materializedJsonSizeLimit)
    {
        try (Response response = client.newCall(request).execute()) {
            // TODO: fix in OkHttp: https://github.com/square/okhttp/issues/3111
            if ((response.code() == 307) || (response.code() == 308)) {
                String location = response.header(LOCATION);
                if (location != null) {
                    request = request.newBuilder().url(location).build();
                    return execute(codec, client, request, materializedJsonSizeLimit);
                }
            }

            ResponseBody responseBody = requireNonNull(response.body());
            if (isJson(responseBody.contentType())) {
                String body = null;
                T value = null;
                IllegalArgumentException exception = null;
                try {
                    if (materializedJsonSizeLimit.isPresent() && (responseBody.contentLength() < 0 || responseBody.contentLength() > materializedJsonSizeLimit.getAsLong())) {
                        // Parse from input stream, response is either of unknown size or too large to materialize. Raw response body
                        // will not be available if parsing fails
                        value = codec.fromJson(responseBody.byteStream());
                    }
                    else {
                        // parse from materialized response body string
                        body = responseBody.string();
                        value = codec.fromJson(body);
                    }
                }
                catch (JsonProcessingException e) {
                    String message;
                    if (body != null) {
                        message = format("Unable to create %s from JSON response:\n[%s]", codec.getType(), body);
                    }
                    else {
                        message = format("Unable to create %s from JSON response", codec.getType());
                    }
                    exception = new IllegalArgumentException(message, e);
                }
                return new JsonResponse<>(response.code(), response.headers(), body, value, exception);
            }
            return new JsonResponse<>(response.code(), response.headers(), responseBody.string());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static boolean isJson(MediaType type)
    {
        return (type != null) && "application".equals(type.type()) && "json".equals(type.subtype());
    }
}
