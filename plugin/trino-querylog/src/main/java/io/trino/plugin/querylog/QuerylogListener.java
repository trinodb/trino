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
package io.trino.plugin.querylog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.airlift.log.Logger;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class QuerylogListener
        implements EventListener
{
    private final Logger log = Logger.get(QuerylogListener.class);

    private final ObjectWriter objectWriter = new ObjectMapper().registerModule(new Jdk8Module()).registerModule(new JavaTimeModule()).writer();

    private final boolean shouldLogCreated;
    private final boolean shouldLogCompleted;
    private final boolean shouldLogSplit;

    private final InetAddress inetAddress;
    private final int port;
    private final String scheme;

    private final Map<String, String> httpHeaders;

    /**
     * Create a HTTP logger of query events
     * @param inetAddress Hostname to destination
     * @param port Port
     * @param scheme Scheme to use, either HTTP or HTTPS
     * @param httpHeaders Map of custom HTTP headers
     * @param shouldLogCreated Enable logging of {@link io.trino.spi.eventlistener.QueryCreatedEvent}
     * @param shouldLogCompleted Enable logging of {@link io.trino.spi.eventlistener.QueryCompletedEvent}
     * @param shouldLogSplit Enable logging of {@link io.trino.spi.eventlistener.SplitCompletedEvent}
     * @throws UnknownHostException When it can't parse InetAddress
     */
    public QuerylogListener(String inetAddress, int port, String scheme, Map<String, String> httpHeaders, boolean shouldLogCreated, boolean shouldLogCompleted, boolean shouldLogSplit)
            throws UnknownHostException
    {
        this.shouldLogCreated = shouldLogCreated;
        this.shouldLogCompleted = shouldLogCompleted;
        this.shouldLogSplit = shouldLogSplit;
        this.httpHeaders = requireNonNull(httpHeaders, "HTTP headers cannot be null");

        if (inetAddress == null || inetAddress.isBlank()) {
            throw new UnknownHostException("Address is empty or blank");
        }
        this.inetAddress = InetAddress.getByName(inetAddress);
        this.scheme = requireNonNull(scheme, "scheme must be either https or http, not null");
        this.port = port;
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        if (shouldLogCreated) {
            sendLog(queryCreatedEvent);
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        if (shouldLogCompleted) {
            sendLog(queryCompletedEvent);
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        if (shouldLogSplit) {
            sendLog(splitCompletedEvent);
        }
    }

    private <T> void sendLog(T event)
    {
        String logJson;

        try {
            logJson = objectWriter.writeValueAsString(event);
        }
        catch (JsonProcessingException e) {
            log.error("Could not parse %s as JSON: %s", event.toString(), e.getMessage());
            return;
        }

        OkHttpClient client = new OkHttpClient();

        RequestBody body = RequestBody.create(MediaType.get("application/json; charset=UTF-8"), logJson);
        Request.Builder requestBuilder = new Request.Builder()
                .url(new HttpUrl.Builder()
                        .host(inetAddress.getHostAddress())
                        .port(port)
                        .scheme(scheme)
                        .build())
                .post(body);
        httpHeaders.forEach(requestBuilder::addHeader);
        Request request = requestBuilder.build();

        try (Response response = client.newCall(request).execute()) {
            if (response.code() != 200) {
                log.warn("Received status code %d from ingest server URL %s when logging %s; expecting status 200", response.code(), request.url().toString(), event.toString());
            }
        }
        catch (IOException e) {
            log.error("Error connecting to ingest sever with URL %s: %s", request.url().toString(), e.getMessage());
        }
    }
}
