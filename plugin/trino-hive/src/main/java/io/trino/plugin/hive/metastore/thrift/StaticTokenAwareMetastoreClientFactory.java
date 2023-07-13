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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.hive.metastore.thrift.FailureAwareThriftMetastoreClient.Callback;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreAuthenticationConfig.ThriftMetastoreAuthenticationType;
import org.apache.thrift.TException;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.metastore.thrift.StaticMetastoreConfig.HIVE_METASTORE_USERNAME;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class StaticTokenAwareMetastoreClientFactory
        implements TokenAwareMetastoreClientFactory
{
    private final List<Backoff> backoffs;
    private final ThriftMetastoreClientFactory clientFactory;
    private final String metastoreUsername;

    @Inject
    public StaticTokenAwareMetastoreClientFactory(StaticMetastoreConfig config, ThriftMetastoreAuthenticationConfig authenticationConfig, ThriftMetastoreClientFactory clientFactory)
    {
        this(config, authenticationConfig, clientFactory, Ticker.systemTicker());
    }

    @VisibleForTesting
    StaticTokenAwareMetastoreClientFactory(StaticMetastoreConfig config, ThriftMetastoreAuthenticationConfig authenticationConfig, ThriftMetastoreClientFactory clientFactory, Ticker ticker)
    {
        this(config.getMetastoreUris(), config.getMetastoreUsername(), clientFactory, ticker);

        checkArgument(
                isNullOrEmpty(metastoreUsername) || authenticationConfig.getAuthenticationType() == ThriftMetastoreAuthenticationType.NONE,
                "%s cannot be used together with %s authentication",
                HIVE_METASTORE_USERNAME,
                authenticationConfig.getAuthenticationType());
    }

    public StaticTokenAwareMetastoreClientFactory(List<URI> metastoreUris, @Nullable String metastoreUsername, ThriftMetastoreClientFactory clientFactory)
    {
        this(metastoreUris, metastoreUsername, clientFactory, Ticker.systemTicker());
    }

    private StaticTokenAwareMetastoreClientFactory(List<URI> metastoreUris, @Nullable String metastoreUsername, ThriftMetastoreClientFactory clientFactory, Ticker ticker)
    {
        requireNonNull(metastoreUris, "metastoreUris is null");
        checkArgument(!metastoreUris.isEmpty(), "metastoreUris must specify at least one URI");
        this.backoffs = metastoreUris.stream()
                .map(StaticTokenAwareMetastoreClientFactory::checkMetastoreUri)
                .map(uri -> HostAndPort.fromParts(uri.getHost(), uri.getPort()))
                .map(address -> new Backoff(address, ticker))
                .collect(toImmutableList());

        this.metastoreUsername = metastoreUsername;
        this.clientFactory = requireNonNull(clientFactory, "clientFactory is null");
    }

    /**
     * Create a metastore client connected to the Hive metastore.
     * <p>
     * As per Hive HA metastore behavior, return the first metastore in the list
     * list of available metastores (i.e. the default metastore) if a connection
     * can be made, else try another of the metastores at random, until either a
     * connection succeeds or there are no more fallback metastores.
     */
    @Override
    public ThriftMetastoreClient createMetastoreClient(Optional<String> delegationToken)
            throws TException
    {
        Comparator<Backoff> comparator = Comparator.comparingLong(Backoff::getBackoffDuration)
                .thenComparingLong(Backoff::getLastFailureTimestamp);
        List<Backoff> backoffsSorted = backoffs.stream()
                .sorted(comparator)
                .collect(toImmutableList());

        TException lastException = null;
        for (Backoff backoff : backoffsSorted) {
            try {
                return getClient(backoff.getAddress(), backoff, delegationToken);
            }
            catch (TException e) {
                lastException = e;
            }
        }

        List<HostAndPort> addresses = backoffsSorted.stream().map(Backoff::getAddress).collect(toImmutableList());
        throw new TException("Failed connecting to Hive metastore: " + addresses, lastException);
    }

    private ThriftMetastoreClient getClient(HostAndPort address, Backoff backoff, Optional<String> delegationToken)
            throws TException
    {
        ThriftMetastoreClient client = new FailureAwareThriftMetastoreClient(clientFactory.create(address, delegationToken), new Callback()
        {
            @Override
            public void success()
            {
                backoff.success();
            }

            @Override
            public void failed(TException e)
            {
                backoff.fail();
            }
        });
        if (!isNullOrEmpty(metastoreUsername)) {
            client.setUGI(metastoreUsername);
        }
        return client;
    }

    private static URI checkMetastoreUri(URI uri)
    {
        requireNonNull(uri, "uri is null");
        String scheme = uri.getScheme();
        checkArgument(!isNullOrEmpty(scheme), "metastoreUri scheme is missing: %s", uri);
        checkArgument(scheme.equals("thrift"), "metastoreUri scheme must be thrift: %s", uri);
        checkArgument(uri.getHost() != null, "metastoreUri host is missing: %s", uri);
        checkArgument(uri.getPort() != -1, "metastoreUri port is missing: %s", uri);
        return uri;
    }

    @VisibleForTesting
    static class Backoff
    {
        static final long MIN_BACKOFF = new Duration(50, MILLISECONDS).roundTo(NANOSECONDS);
        static final long MAX_BACKOFF = new Duration(60, SECONDS).roundTo(NANOSECONDS);

        private final HostAndPort address;
        private final Ticker ticker;
        private long backoffDuration = MIN_BACKOFF;
        private OptionalLong lastFailureTimestamp = OptionalLong.empty();

        Backoff(HostAndPort address, Ticker ticker)
        {
            this.address = requireNonNull(address, "address is null");
            this.ticker = requireNonNull(ticker, "ticker is null");
        }

        public HostAndPort getAddress()
        {
            return address;
        }

        synchronized void fail()
        {
            lastFailureTimestamp = OptionalLong.of(ticker.read());
            backoffDuration = min(backoffDuration * 2, MAX_BACKOFF);
        }

        synchronized void success()
        {
            lastFailureTimestamp = OptionalLong.empty();
            backoffDuration = MIN_BACKOFF;
        }

        synchronized long getLastFailureTimestamp()
        {
            return lastFailureTimestamp.orElse(Long.MIN_VALUE);
        }

        synchronized long getBackoffDuration()
        {
            if (lastFailureTimestamp.isEmpty()) {
                return 0;
            }
            long timeSinceLastFail = ticker.read() - lastFailureTimestamp.getAsLong();
            return max(backoffDuration - timeSinceLastFail, 0);
        }
    }
}
