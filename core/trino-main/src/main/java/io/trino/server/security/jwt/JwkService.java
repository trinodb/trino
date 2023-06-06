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
package io.trino.server.security.jwt;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closer;
import io.airlift.concurrent.Threads;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import javax.annotation.processing.Generated;

import java.io.IOException;
import java.net.URI;
import java.security.PublicKey;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.server.security.jwt.JwkDecoder.decodeKeys;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public final class JwkService
{
    private static final Logger log = Logger.get(JwkService.class);

    private final URI address;
    private final HttpClient httpClient;
    private final Duration refreshDelay;
    private final AtomicReference<Map<String, PublicKey>> keys;

    @Generated("this")
    private Closer closer;

    public JwkService(URI address, HttpClient httpClient)
    {
        this(address, httpClient, new Duration(15, TimeUnit.MINUTES));
    }

    @VisibleForTesting
    public JwkService(URI address, HttpClient httpClient, Duration refreshDelay)
    {
        this.address = requireNonNull(address, "address is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.refreshDelay = requireNonNull(refreshDelay, "refreshDelay is null");

        this.keys = new AtomicReference<>(fetchKeys());
    }

    @PostConstruct
    public synchronized void start()
    {
        if (closer != null) {
            return;
        }
        closer = Closer.create();

        ScheduledExecutorService executorService = newSingleThreadScheduledExecutor(Threads.daemonThreadsNamed("JWK loader"));
        closer.register(executorService::shutdownNow);

        ScheduledFuture<?> refreshJob = executorService.scheduleWithFixedDelay(
                () -> {
                    try {
                        refreshKeys();
                    }
                    catch (Exception e) {
                        log.error(e, "Error fetching JWK keys");
                    }
                },
                refreshDelay.toMillis(),
                refreshDelay.toMillis(),
                TimeUnit.MILLISECONDS);
        closer.register(() -> refreshJob.cancel(true));
    }

    @PreDestroy
    public synchronized void stop()
    {
        if (closer == null) {
            return;
        }
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new RuntimeException("Error stopping JWK service", e);
        }
        finally {
            closer = null;
        }
    }

    public Map<String, PublicKey> getKeys()
    {
        return keys.get();
    }

    public Optional<PublicKey> getKey(String keyId)
    {
        return Optional.ofNullable(keys.get().get(keyId));
    }

    public void refreshKeys()
            throws RuntimeException
    {
        keys.set(fetchKeys());
    }

    private Map<String, PublicKey> fetchKeys()
            throws RuntimeException
    {
        Request request = prepareGet().setUri(address).build();
        StringResponse response;
        try {
            response = httpClient.execute(request, createStringResponseHandler());
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Error reading JWK keys from " + address, e);
        }
        if (response.getStatusCode() != 200) {
            throw new RuntimeException("Unexpected response code " + response.getStatusCode() + " from JWK service at " + address);
        }
        try {
            return decodeKeys(response.getBody());
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Unable to decode JWK response from " + address, e);
        }
    }
}
