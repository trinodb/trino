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
package io.trino.plugin.hive.util;

import io.trino.plugin.hive.HiveCompressionCodec;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;

import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.hive.util.CompressionConfigUtil.assertCompressionConfigured;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCompressionConfigUtil
{
    @Test(dataProvider = "compressionCodes")
    public void testAssertCompressionConfigured(HiveCompressionCodec compressionCodec)
    {
        Configuration config = newEmptyConfiguration();
        assertThatThrownBy(() -> assertCompressionConfigured(config))
                .hasMessage("Compression should have been configured");

        CompressionConfigUtil.configureCompression(config, compressionCodec);
        assertCompressionConfigured(config); // ok now
    }

    @DataProvider
    public Object[][] compressionCodes()
    {
        return Arrays.stream(HiveCompressionCodec.values())
                .map(codec -> new Object[] {codec})
                .toArray(Object[][]::new);
    }
}
