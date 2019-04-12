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

import io.airlift.units.Duration;
import io.prestosql.spi.PrestoException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.airlift.testing.Assertions.assertContains;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class DiscoveryMetastoreLocatorTest
{
    private static final ThriftMetastoreClient DEFAULT_CLIENT = createFakeMetastoreClient();
    private static final ThriftMetastoreClient FALLBACK_CLIENT = createFakeMetastoreClient();

    private static final DiscoveryMetastoreConfig CONFIG_WITH_FALLBACK = new DiscoveryMetastoreConfig()
            .setMetastoreUris("thrift://default:8080,thrift://fallback:8090,thrift://fallback2:8090");
    private static final DiscoveryMetastoreConfig CONFIG_WITHOUT_FALLBACK = new DiscoveryMetastoreConfig()
            .setMetastoreUris("thrift://default:8080");
    private static final DiscoveryMetastoreConfig CONFIG_WITH_FALLBACK_WITH_CONSUL = new DiscoveryMetastoreConfig()
            .setMetastoreUris("consul://default:8080/hive-metastore,thrift://fallback:8090,thrift://fallback2:8090");
    private static final DiscoveryMetastoreConfig CONFIG_WITHOUT_FALLBACK_WITH_CONSUL = new DiscoveryMetastoreConfig()
            .setMetastoreUris("consul://default:8080/hive-metastore");

    @Test
    public void testDefaultHiveMetastore()
    {
        DiscoveryMetastoreLocator locator = createLocator(CONFIG_WITH_FALLBACK, singletonList(DEFAULT_CLIENT));
        assertEquals(locator.createMetastoreClient(), DEFAULT_CLIENT);
    }

    @Test
    public void testFallbackHiveMetastore()
    {
        DiscoveryMetastoreLocator locator = createLocator(CONFIG_WITH_FALLBACK, asList(null, null, FALLBACK_CLIENT, null, null, FALLBACK_CLIENT));
        assertEquals(locator.createMetastoreClient(), FALLBACK_CLIENT);
        assertEquals(locator.createMetastoreClient(), FALLBACK_CLIENT);
    }

    @Test
    public void testFallbackHiveMetastoreFails()
    {
        DiscoveryMetastoreLocator locator = createLocator(CONFIG_WITH_FALLBACK, asList(null, null, null));
        assertCreateClientFails(locator, "Failed connecting to Hive metastore using any of the URI's: [thrift://default:8080, thrift://fallback:8090, thrift://fallback2:8090]");
    }

    @Test
    public void testMetastoreFailedWithoutFallback()
    {
        DiscoveryMetastoreLocator locator = createLocator(CONFIG_WITHOUT_FALLBACK, singletonList(null));
        assertCreateClientFails(locator, "Failed connecting to Hive metastore using any of the URI's: [thrift://default:8080]");
    }

    @Test
    public void testFallbackHiveMetastoreWithConsul()
    {
        DiscoveryMetastoreLocator locator = createLocator(CONFIG_WITH_FALLBACK_WITH_CONSUL, asList(null, FALLBACK_CLIENT, null, FALLBACK_CLIENT));
        assertEquals(locator.createMetastoreClient(), FALLBACK_CLIENT);
        assertEquals(locator.createMetastoreClient(), FALLBACK_CLIENT);
    }

    @Test
    public void testMetastoreFailedWithoutFallbackWithConsul()
    {
        DiscoveryMetastoreLocator locator = createLocator(CONFIG_WITHOUT_FALLBACK_WITH_CONSUL, singletonList(null));
        assertCreateClientFails(locator, "Failed to resolve Hive metastore addresses: [consul://default:8080/hive-metastore");
    }

    private static void assertCreateClientFails(DiscoveryMetastoreLocator locator, String message)
    {
        try {
            locator.createMetastoreClient();
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertContains(e.getMessage(), message);
        }
    }

    private static DiscoveryMetastoreLocator createLocator(DiscoveryMetastoreConfig config, List<ThriftMetastoreClient> clients)
    {
        return new DiscoveryMetastoreLocator(
                config,
                new MockHiveMetastoreClientFactory(Optional.empty(), new Duration(1, SECONDS), clients));
    }

    private static ThriftMetastoreClient createFakeMetastoreClient()
    {
        return new MockThriftMetastoreClient();
    }
}
