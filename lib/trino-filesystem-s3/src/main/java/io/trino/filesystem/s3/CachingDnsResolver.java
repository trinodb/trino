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
package io.trino.filesystem.s3;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.trino.cache.EvictableCacheBuilder;
import org.apache.hc.client5.http.DnsResolver;
import org.apache.hc.client5.http.SystemDefaultDnsResolver;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Security;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

/**
 * Returns cached host addresses and refreshes them on a background executor once they are older than
 * the JVM address cache TTL, so a slow DNS lookup does not block the thread opening a connection.
 * A host is resolved on the calling thread when it is first used, after a failed refresh, and after
 * it is unused for longer than the TTL plus {@link #MAX_STALENESS}. Each call starts at the next
 * address, so connections spread over all addresses of a host.
 */
final class CachingDnsResolver
        implements DnsResolver
{
    // Matches the default in sun.net.InetAddressCachePolicy
    private static final int DEFAULT_JVM_CACHE_TTL_SECONDS = 30;
    // Addresses older than the TTL are served while a refresh runs, for at most this long past the TTL
    @VisibleForTesting
    static final Duration MAX_STALENESS = Duration.ofMinutes(1);
    private static final long MAX_HOSTS = 1000;

    private final DnsResolver delegate;
    // The JVM entry from our own lookup is always expired when the refresh runs, but another lookup
    // in the JVM can refill it, so a busy host can lag by up to twice the TTL
    private final LoadingCache<String, Optional<HostAddresses>> addresses;

    /**
     * Returns empty when the JVM caches addresses never or forever, so lookups go to the JVM as is.
     */
    public static Optional<CachingDnsResolver> create(Executor refreshExecutor)
    {
        int ttlSeconds = jvmCacheTtlSeconds(Security.getProperty("networkaddress.cache.ttl"), System.getProperty("sun.net.inetaddr.ttl"));
        if (ttlSeconds <= 0) {
            return Optional.empty();
        }
        return Optional.of(new CachingDnsResolver(SystemDefaultDnsResolver.INSTANCE, refreshExecutor, Ticker.systemTicker(), Duration.ofSeconds(ttlSeconds)));
    }

    // Reads the TTL the way sun.net.InetAddressCachePolicy does
    @VisibleForTesting
    static int jvmCacheTtlSeconds(String securityProperty, String systemProperty)
    {
        if (securityProperty != null) {
            try {
                return Integer.parseInt(securityProperty);
            }
            catch (NumberFormatException _) {
                // The JVM falls back to the system property
            }
        }
        if (systemProperty != null) {
            try {
                return Integer.decode(systemProperty);
            }
            catch (NumberFormatException _) {
                // The JVM falls back to the default
            }
        }
        return DEFAULT_JVM_CACHE_TTL_SECONDS;
    }

    @VisibleForTesting
    CachingDnsResolver(DnsResolver delegate, Executor refreshExecutor, Ticker ticker, Duration ttl)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.addresses = EvictableCacheBuilder.newBuilder()
                .ticker(ticker)
                .refreshAfterWrite(ttl)
                .expireAfterWrite(ttl.plus(MAX_STALENESS))
                .maximumSize(MAX_HOSTS)
                .build(asyncReloading(new CacheLoader<>()
                {
                    @Override
                    public Optional<HostAddresses> load(String host)
                            throws UnknownHostException
                    {
                        return Optional.of(new HostAddresses(delegate.resolve(host)));
                    }

                    // An empty value marks a failed refresh
                    @Override
                    public ListenableFuture<Optional<HostAddresses>> reload(String host, Optional<HostAddresses> oldValue)
                    {
                        try {
                            return immediateFuture(load(host));
                        }
                        catch (UnknownHostException _) {
                            return immediateFuture(Optional.empty());
                        }
                    }
                }, refreshExecutor));
    }

    @Override
    public InetAddress[] resolve(String host)
            throws UnknownHostException
    {
        Optional<HostAddresses> cached = get(host);
        if (cached.isEmpty()) {
            addresses.asMap().remove(host, cached);
            cached = get(host);
        }
        return cached.orElseThrow().rotate();
    }

    @Override
    public String resolveCanonicalHostname(String host)
            throws UnknownHostException
    {
        return delegate.resolveCanonicalHostname(host);
    }

    private Optional<HostAddresses> get(String host)
            throws UnknownHostException
    {
        try {
            return addresses.get(host);
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            Throwable cause = e.getCause();
            throwIfInstanceOf(cause, UnknownHostException.class);
            throwIfUnchecked(cause);
            throw new UncheckedExecutionException(cause);
        }
    }

    // Addresses of one host and the offset the next call starts at
    private static final class HostAddresses
    {
        private final List<InetAddress> addresses;
        private final AtomicInteger nextOffset = new AtomicInteger();

        HostAddresses(InetAddress[] addresses)
        {
            this.addresses = ImmutableList.copyOf(addresses);
        }

        // Starts each connection at the next address of the leading run that shares the first address's family
        InetAddress[] rotate()
        {
            InetAddress[] rotated = addresses.toArray(InetAddress[]::new);
            int sameFamily = 1;
            while (sameFamily < rotated.length && rotated[sameFamily].getClass() == rotated[0].getClass()) {
                sameFamily++;
            }
            int offset = Math.floorMod(nextOffset.getAndIncrement(), sameFamily);
            for (int i = 0; i < sameFamily; i++) {
                rotated[i] = addresses.get((offset + i) % sameFamily);
            }
            return rotated;
        }
    }
}
