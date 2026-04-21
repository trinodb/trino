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
package io.trino.plugin.exchange.filesystem.alluxio;

import alluxio.client.WriteType;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static com.google.common.io.Resources.getResource;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExchangeAlluxioConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ExchangeAlluxioConfig.class)
                .setAlluxioStorageBlockSize(DataSize.of(4, MEGABYTE))
                .setAlluxioSiteConfPath(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        String alluxioSiteConfPath = getResource(getClass(), "alluxio-site.properties").getPath();
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("exchange.alluxio.block-size", "8MB")
                .put("exchange.alluxio.site-file-path", alluxioSiteConfPath)
                .buildOrThrow();

        ExchangeAlluxioConfig expected = new ExchangeAlluxioConfig()
                .setAlluxioStorageBlockSize(DataSize.of(8, MEGABYTE))
                .setAlluxioSiteConfPath(alluxioSiteConfPath);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testAlluxioSiteConf()
    {
        // test default alluxio config
        ExchangeAlluxioConfig defaultExchangeAlluxioConfig = new ExchangeAlluxioConfig();
        AlluxioConfiguration defaultAlluxioConfiguration = AlluxioFileSystemExchangeStorage
                .loadAlluxioConfiguration(defaultExchangeAlluxioConfig.getAlluxioSiteConfPath());
        assertThat(defaultAlluxioConfiguration.get(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT))
                .isEqualTo(WriteType.ASYNC_THROUGH);

        // test custom alluxio config
        String alluxioSiteConfPath = getResource(getClass(), "alluxio-site.properties").getPath();
        ExchangeAlluxioConfig exchangeAlluxioConfig = new ExchangeAlluxioConfig()
                .setAlluxioSiteConfPath(alluxioSiteConfPath);
        AlluxioConfiguration alluxioConfiguration = AlluxioFileSystemExchangeStorage
                .loadAlluxioConfiguration(exchangeAlluxioConfig.getAlluxioSiteConfPath());
        assertThat(alluxioConfiguration.get(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT))
                .isEqualTo(WriteType.MUST_CACHE);
    }
}
