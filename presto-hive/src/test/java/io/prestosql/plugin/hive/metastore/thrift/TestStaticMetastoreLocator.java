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
import org.apache.thrift.TException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestStaticMetastoreLocator
{
    private static final ThriftMetastoreClient DEFAULT_CLIENT = createFakeMetastoreClient();
    private static final ThriftMetastoreClient FALLBACK_CLIENT = createFakeMetastoreClient();

    private static final StaticMetastoreConfig CONFIG_WITH_FALLBACK = new StaticMetastoreConfig()
            .setMetastoreUris("thrift://default:8080,thrift://fallback:8090,thrift://fallback2:8090");

    private static final StaticMetastoreConfig CONFIG_WITHOUT_FALLBACK = new StaticMetastoreConfig()
            .setMetastoreUris("thrift://default:8080");

    private static final StaticMetastoreConfig CONFIG_WITH_FALLBACK_WITH_USER = new StaticMetastoreConfig()
            .setMetastoreUris("thrift://default:8080,thrift://fallback:8090,thrift://fallback2:8090")
            .setMetastoreUsername("presto");

    private static final StaticMetastoreConfig CONFIG_WITHOUT_FALLBACK_WITH_USER = new StaticMetastoreConfig()
            .setMetastoreUris("thrift://default:8080")
            .setMetastoreUsername("presto");

    @Test
    public void testDefaultHiveMetastore()
            throws TException
    {
        MetastoreLocator locator = createMetastoreLocator(CONFIG_WITH_FALLBACK, singletonList(DEFAULT_CLIENT));
        assertEquals(locator.createMetastoreClient(), DEFAULT_CLIENT);
    }

    @Test
    public void testFallbackHiveMetastore()
            throws TException
    {
        MetastoreLocator locator = createMetastoreLocator(CONFIG_WITH_FALLBACK, asList(null, null, FALLBACK_CLIENT));
        assertEquals(locator.createMetastoreClient(), FALLBACK_CLIENT);
    }

    @Test
    public void testFallbackHiveMetastoreFails()
    {
        MetastoreLocator locator = createMetastoreLocator(CONFIG_WITH_FALLBACK, asList(null, null, null));
        assertCreateClientFails(locator, "Failed connecting to Hive metastore: [default:8080, fallback:8090, fallback2:8090]");
    }

    @Test
    public void testMetastoreFailedWithoutFallback()
    {
        MetastoreLocator locator = createMetastoreLocator(CONFIG_WITHOUT_FALLBACK, singletonList(null));
        assertCreateClientFails(locator, "Failed connecting to Hive metastore: [default:8080]");
    }

    @Test
    public void testFallbackHiveMetastoreWithHiveUser()
            throws TException
    {
        MetastoreLocator locator = createMetastoreLocator(CONFIG_WITH_FALLBACK_WITH_USER, asList(null, null, FALLBACK_CLIENT));
        assertEquals(locator.createMetastoreClient(), FALLBACK_CLIENT);
    }

    @Test
    public void testMetastoreFailedWithoutFallbackWithHiveUser()
    {
        MetastoreLocator locator = createMetastoreLocator(CONFIG_WITHOUT_FALLBACK_WITH_USER, singletonList(null));
        assertCreateClientFails(locator, "Failed connecting to Hive metastore: [default:8080]");
    }

    private static void assertCreateClientFails(MetastoreLocator locator, String message)
    {
        assertThatThrownBy(locator::createMetastoreClient)
                .hasCauseInstanceOf(TException.class)
                .hasMessage(message);
    }

    private static MetastoreLocator createMetastoreLocator(StaticMetastoreConfig config, List<ThriftMetastoreClient> clients)
    {
        return new StaticMetastoreLocator(config, new MockHiveMetastoreClientFactory(Optional.empty(), new Duration(1, SECONDS), clients));
    }

    private static ThriftMetastoreClient createFakeMetastoreClient()
    {
        return new MockThriftMetastoreClient();
    }
}
