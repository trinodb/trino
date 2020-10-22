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
package io.prestosql.plugin.hive.metastore.thrift;

import com.google.common.base.Ticker;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.metastore.thrift.FailureAwareThriftMetastoreClient.Callback;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreAuthenticationConfig.ThriftMetastoreAuthenticationType;
import org.apache.thrift.TException;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.metastore.thrift.StaticMetastoreConfig.HIVE_METASTORE_USERNAME;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class StaticMetastoreLocator
        implements MetastoreLocator
{
    private final List<HostAndPort> addresses;
    private final List<Backoff> backoffs;
    private final ThriftMetastoreClientFactory clientFactory;
    private final String metastoreUsername;

    @Inject
    public StaticMetastoreLocator(StaticMetastoreConfig config, ThriftMetastoreAuthenticationConfig authenticationConfig, ThriftMetastoreClientFactory clientFactory)
    {
        this(config.getMetastoreUris(), config.getMetastoreUsername(), clientFactory);

        checkArgument(
                isNullOrEmpty(metastoreUsername) || authenticationConfig.getAuthenticationType() == ThriftMetastoreAuthenticationType.NONE,
                "%s cannot be used together with %s authentication",
                HIVE_METASTORE_USERNAME,
                authenticationConfig.getAuthenticationType());
    }

    public StaticMetastoreLocator(List<URI> metastoreUris, @Nullable String metastoreUsername, ThriftMetastoreClientFactory clientFactory)
    {
        requireNonNull(metastoreUris, "metastoreUris is null");
        checkArgument(!metastoreUris.isEmpty(), "metastoreUris must specify at least one URI");
        this.addresses = metastoreUris.stream()
                .map(StaticMetastoreLocator::checkMetastoreUri)
                .map(uri -> HostAndPort.fromParts(uri.getHost(), uri.getPort()))
                .collect(toList());
        this.backoffs = IntStream.range(0, addresses.size()).mapToObj(ignore -> new Backoff()).collect(toImmutableList());
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
        List<Integer> indices = backoffs.stream()
                .sorted(Comparator.comparingLong(Backoff::getBackoffDuration))
                .map(backoffs::indexOf)
                .collect(toImmutableList());

        TException lastException = null;
        for (int index : indices) {
            try {
                return getClient(addresses.get(index), backoffs.get(index), delegationToken);
            }
            catch (TException e) {
                lastException = e;
            }
        }
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
        requireNonNull(uri, "metastoreUri is null");
        String scheme = uri.getScheme();
        checkArgument(!isNullOrEmpty(scheme), "metastoreUri scheme is missing: %s", uri);
        checkArgument(scheme.equals("thrift"), "metastoreUri scheme must be thrift: %s", uri);
        checkArgument(uri.getHost() != null, "metastoreUri host is missing: %s", uri);
        checkArgument(uri.getPort() != -1, "metastoreUri port is missing: %s", uri);
        return uri;
    }

    private static class Backoff
    {
        private static final long MIN_BACKOFF = new Duration(50, MILLISECONDS).roundTo(NANOSECONDS);
        private static final long MAX_BACKOFF = new Duration(60, SECONDS).roundTo(NANOSECONDS);

        private final Ticker ticker = Ticker.systemTicker();
        private long backoffDuration = MIN_BACKOFF;
        private OptionalLong lastFailureTimestamps = OptionalLong.empty();

        synchronized void fail()
        {
            lastFailureTimestamps = OptionalLong.of(ticker.read());
            backoffDuration = min(backoffDuration * 2, MAX_BACKOFF);
        }

        synchronized void success()
        {
            lastFailureTimestamps = OptionalLong.empty();
            backoffDuration = MIN_BACKOFF;
        }

        synchronized long getBackoffDuration()
        {
            if (lastFailureTimestamps.isPresent()) {
                long timeSinceLastFail = ticker.read() - lastFailureTimestamps.getAsLong();
                return max(backoffDuration - timeSinceLastFail, 0);
            }
            return 0;
        }
    }
}
