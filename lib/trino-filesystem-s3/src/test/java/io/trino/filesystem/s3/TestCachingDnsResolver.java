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

import io.airlift.testing.TestingTicker;
import org.apache.hc.client5.http.DnsResolver;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.filesystem.s3.CachingDnsResolver.MAX_STALENESS;
import static io.trino.filesystem.s3.CachingDnsResolver.jvmCacheTtlSeconds;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestCachingDnsResolver
{
    private static final String HOST = "bucket.s3.us-east-1.amazonaws.com";
    private static final String OTHER_HOST = "other-bucket.s3.us-east-1.amazonaws.com";
    private static final InetAddress FIRST_ADDRESS = address(1);
    private static final InetAddress SECOND_ADDRESS = address(2);
    private static final InetAddress THIRD_ADDRESS = address(3);
    private static final InetAddress FOURTH_ADDRESS = address(4);
    private static final InetAddress IPV6_ADDRESS = address(new byte[] {(byte) 0xFD, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1});
    private static final Duration TTL = Duration.ofSeconds(30);

    private final TestingDnsResolver delegate = new TestingDnsResolver();
    private final QueuedExecutor refreshExecutor = new QueuedExecutor();
    private final TestingTicker ticker = new TestingTicker();
    private final CachingDnsResolver resolver = new CachingDnsResolver(delegate, refreshExecutor, ticker, TTL);

    @Test
    void testFirstLookupResolvesOnCallingThread()
            throws UnknownHostException
    {
        delegate.setAddress(HOST, FIRST_ADDRESS);

        assertThat(resolver.resolve(HOST)).containsExactly(FIRST_ADDRESS);
        assertThat(delegate.lookups()).isEqualTo(1);
        assertThat(refreshExecutor.pendingTasks()).isZero();
    }

    @Test
    void testCachedAddressesWithinRefreshInterval()
            throws UnknownHostException
    {
        delegate.setAddress(HOST, FIRST_ADDRESS);
        resolver.resolve(HOST);
        delegate.setAddress(HOST, SECOND_ADDRESS);
        advance(TTL.minusSeconds(1));

        assertThat(resolver.resolve(HOST)).containsExactly(FIRST_ADDRESS);
        assertThat(delegate.lookups()).isEqualTo(1);
        assertThat(refreshExecutor.pendingTasks()).isZero();
    }

    @Test
    void testStaleAddressesReturnedWhileRefreshing()
            throws UnknownHostException
    {
        delegate.setAddress(HOST, FIRST_ADDRESS);
        resolver.resolve(HOST);
        delegate.setAddress(HOST, SECOND_ADDRESS);
        advance(TTL.plusSeconds(1));

        assertThat(resolver.resolve(HOST)).containsExactly(FIRST_ADDRESS);
        assertThat(resolver.resolve(HOST)).containsExactly(FIRST_ADDRESS);
        assertThat(delegate.lookups()).isEqualTo(1);
        assertThat(refreshExecutor.pendingTasks()).isEqualTo(1);

        refreshExecutor.runAll();
        assertThat(delegate.lookups()).isEqualTo(2);
        assertThat(resolver.resolve(HOST)).containsExactly(SECOND_ADDRESS);
        assertThat(refreshExecutor.pendingTasks()).isZero();
    }

    @Test
    void testFailedRefreshDropsAddresses()
            throws UnknownHostException
    {
        delegate.setAddress(HOST, FIRST_ADDRESS);
        resolver.resolve(HOST);
        delegate.removeAddress(HOST);
        advance(TTL.plusSeconds(1));

        assertThat(resolver.resolve(HOST)).containsExactly(FIRST_ADDRESS);
        refreshExecutor.runAll();
        assertThat(delegate.lookups()).isEqualTo(2);
        assertThatThrownBy(() -> resolver.resolve(HOST))
                .isInstanceOf(UnknownHostException.class)
                .hasMessage(HOST);
        assertThat(delegate.lookups()).isEqualTo(3);

        delegate.setAddress(HOST, SECOND_ADDRESS);
        assertThat(resolver.resolve(HOST)).containsExactly(SECOND_ADDRESS);
        assertThat(delegate.lookups()).isEqualTo(4);
        assertThat(refreshExecutor.pendingTasks()).isZero();
    }

    @Test
    void testExpiredAddressesResolveOnCallingThread()
            throws UnknownHostException
    {
        delegate.setAddress(HOST, FIRST_ADDRESS);
        resolver.resolve(HOST);
        delegate.setAddress(HOST, SECOND_ADDRESS);
        advance(TTL.plus(MAX_STALENESS).plusSeconds(1));

        assertThat(resolver.resolve(HOST)).containsExactly(SECOND_ADDRESS);
        assertThat(delegate.lookups()).isEqualTo(2);
        assertThat(refreshExecutor.pendingTasks()).isZero();
    }

    @Test
    void testUnknownHost()
    {
        assertThatThrownBy(() -> resolver.resolve(HOST))
                .isInstanceOf(UnknownHostException.class)
                .hasMessage(HOST);
    }

    @Test
    void testLookupRuntimeExceptionPropagates()
    {
        IllegalArgumentException failure = new IllegalArgumentException("invalid host");
        delegate.setFailure(failure);

        assertThatThrownBy(() -> resolver.resolve(HOST)).isSameAs(failure);
    }

    @Test
    void testRotatesAddresses()
            throws UnknownHostException
    {
        delegate.setAddress(HOST, FIRST_ADDRESS, SECOND_ADDRESS, THIRD_ADDRESS);

        assertThat(resolver.resolve(HOST)).containsExactly(FIRST_ADDRESS, SECOND_ADDRESS, THIRD_ADDRESS);
        assertThat(resolver.resolve(HOST)).containsExactly(SECOND_ADDRESS, THIRD_ADDRESS, FIRST_ADDRESS);
        assertThat(resolver.resolve(HOST)).containsExactly(THIRD_ADDRESS, FIRST_ADDRESS, SECOND_ADDRESS);
        assertThat(resolver.resolve(HOST)).containsExactly(FIRST_ADDRESS, SECOND_ADDRESS, THIRD_ADDRESS);
        assertThat(delegate.lookups()).isEqualTo(1);
    }

    @Test
    void testRotationKeepsFirstAddressFamily()
            throws UnknownHostException
    {
        delegate.setAddress(HOST, FIRST_ADDRESS, SECOND_ADDRESS, IPV6_ADDRESS);

        assertThat(resolver.resolve(HOST)).containsExactly(FIRST_ADDRESS, SECOND_ADDRESS, IPV6_ADDRESS);
        assertThat(resolver.resolve(HOST)).containsExactly(SECOND_ADDRESS, FIRST_ADDRESS, IPV6_ADDRESS);
        assertThat(resolver.resolve(HOST)).containsExactly(FIRST_ADDRESS, SECOND_ADDRESS, IPV6_ADDRESS);
    }

    @Test
    void testRotatesEachHostSeparately()
            throws UnknownHostException
    {
        delegate.setAddress(HOST, FIRST_ADDRESS, SECOND_ADDRESS);
        delegate.setAddress(OTHER_HOST, THIRD_ADDRESS, FOURTH_ADDRESS);

        assertThat(resolver.resolve(HOST)).containsExactly(FIRST_ADDRESS, SECOND_ADDRESS);
        assertThat(resolver.resolve(OTHER_HOST)).containsExactly(THIRD_ADDRESS, FOURTH_ADDRESS);
        assertThat(resolver.resolve(HOST)).containsExactly(SECOND_ADDRESS, FIRST_ADDRESS);
        assertThat(resolver.resolve(OTHER_HOST)).containsExactly(FOURTH_ADDRESS, THIRD_ADDRESS);
    }

    @Test
    void testJvmCacheTtl()
    {
        assertThat(jvmCacheTtlSeconds(null, null)).isEqualTo(30);
        assertThat(jvmCacheTtlSeconds("60", "120")).isEqualTo(60);
        assertThat(jvmCacheTtlSeconds("invalid", "120")).isEqualTo(120);
        assertThat(jvmCacheTtlSeconds(null, "0x10")).isEqualTo(16);
        assertThat(jvmCacheTtlSeconds(null, "invalid")).isEqualTo(30);
        assertThat(jvmCacheTtlSeconds("0", null)).isZero();
        assertThat(jvmCacheTtlSeconds(null, "-1")).isEqualTo(-1);
    }

    private void advance(Duration duration)
    {
        ticker.increment(duration.toNanos(), NANOSECONDS);
    }

    private static InetAddress address(int lastOctet)
    {
        return address(new byte[] {10, 0, 0, (byte) lastOctet});
    }

    private static InetAddress address(byte[] address)
    {
        try {
            return InetAddress.getByAddress(HOST, address);
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private static class TestingDnsResolver
            implements DnsResolver
    {
        private final Map<String, InetAddress[]> addresses = new ConcurrentHashMap<>();
        private final AtomicInteger lookups = new AtomicInteger();
        private volatile RuntimeException failure;

        void setAddress(String host, InetAddress... hostAddresses)
        {
            addresses.put(host, hostAddresses.clone());
        }

        void removeAddress(String host)
        {
            addresses.remove(host);
        }

        void setFailure(RuntimeException failure)
        {
            this.failure = failure;
        }

        int lookups()
        {
            return lookups.get();
        }

        @Override
        public InetAddress[] resolve(String host)
                throws UnknownHostException
        {
            lookups.incrementAndGet();
            if (failure != null) {
                throw failure;
            }
            InetAddress[] hostAddresses = addresses.get(host);
            if (hostAddresses == null) {
                throw new UnknownHostException(host);
            }
            return hostAddresses.clone();
        }

        @Override
        public String resolveCanonicalHostname(String host)
        {
            return host;
        }
    }

    private static class QueuedExecutor
            implements Executor
    {
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable task)
        {
            tasks.add(task);
        }

        int pendingTasks()
        {
            return tasks.size();
        }

        void runAll()
        {
            while (!tasks.isEmpty()) {
                tasks.remove().run();
            }
        }
    }
}
