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
package io.trino.plugin.opa;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.util.Objects.requireNonNull;

public final class HttpClientUtils
{
    private HttpClientUtils() {}

    public static class RecordingHttpProcessor
            implements TestingHttpClient.Processor
    {
        private static final JsonMapper jsonMapper = new JsonMapper();
        private final List<JsonNode> requests = Collections.synchronizedList(new ArrayList<>());
        private final Function<JsonNode, MockResponse> handler;
        private final URI expectedURI;
        private final String expectedMethod;
        private final String expectedContentType;

        public RecordingHttpProcessor(URI expectedURI, String expectedMethod, String expectedContentType, Function<JsonNode, MockResponse> handler)
        {
            this.expectedMethod = requireNonNull(expectedMethod, "expectedMethod is null");
            this.expectedContentType = requireNonNull(expectedContentType, "expectedContentType is null");
            this.expectedURI = requireNonNull(expectedURI, "expectedURI is null");
            this.handler = requireNonNull(handler, "handler is null");
        }

        @Override
        public Response handle(Request request)
        {
            if (!requireNonNull(request.getMethod()).equalsIgnoreCase(expectedMethod)) {
                throw new IllegalArgumentException("Unexpected method: %s".formatted(request.getMethod()));
            }
            String actualContentType = request.getHeader(CONTENT_TYPE);
            if (!requireNonNull(actualContentType).equalsIgnoreCase(expectedContentType)) {
                throw new IllegalArgumentException("Unexpected content type header: %s".formatted(actualContentType));
            }
            if (!requireNonNull(request.getUri()).equals(expectedURI)) {
                throw new IllegalArgumentException("Unexpected URI: %s".formatted(request.getUri().toString()));
            }
            if (requireNonNull(request.getBodyGenerator()) instanceof StaticBodyGenerator bodyGenerator) {
                String requestContents = new String(bodyGenerator.getBody(), StandardCharsets.UTF_8);
                try {
                    JsonNode parsedRequest = jsonMapper.readTree(requestContents);
                    requests.add(parsedRequest);
                    return handler.apply(parsedRequest).buildResponse();
                }
                catch (IOException e) {
                    throw new IllegalArgumentException("Request has illegal JSON", e);
                }
            }
            else {
                throw new IllegalArgumentException("Request has an unexpected body generator");
            }
        }

        public List<JsonNode> getRequests()
        {
            return ImmutableList.copyOf(requests);
        }
    }

    public static final class InstrumentedHttpClient
            extends TestingHttpClient
    {
        private final RecordingHttpProcessor httpProcessor;

        public InstrumentedHttpClient(URI expectedURI, String expectedMethod, String expectedContentType, Function<JsonNode, MockResponse> handler)
        {
            this(new RecordingHttpProcessor(expectedURI, expectedMethod, expectedContentType, handler));
        }

        public InstrumentedHttpClient(RecordingHttpProcessor processor)
        {
            super(processor);
            this.httpProcessor = processor;
        }

        public List<JsonNode> getRequests()
        {
            return httpProcessor.getRequests();
        }
    }

    public record MockResponse(String contents, int statusCode)
    {
        public TestingResponse buildResponse()
        {
            return new TestingResponse(
                    HttpStatus.fromStatusCode(this.statusCode),
                    ImmutableListMultimap.of(CONTENT_TYPE, JSON_UTF_8.toString()),
                    this.contents.getBytes(StandardCharsets.UTF_8));
        }
    }
}
