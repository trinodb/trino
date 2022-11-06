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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestBaseJdbcConfig
{
    private static final Duration ZERO = Duration.succinctDuration(0, MINUTES);

    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(BaseJdbcConfig.class)
                .setConnectionUrl(null)
                .setJdbcTypesMappedToVarchar("")
                .setMetadataCacheTtl(ZERO)
                .setCacheMissing(false)
                .setCacheMaximumSize(10000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("connection-url", "jdbc:h2:mem:config")
                .put("jdbc-types-mapped-to-varchar", "mytype,struct_type1")
                .put("metadata.cache-ttl", "1s")
                .put("metadata.cache-missing", "true")
                .put("metadata.cache-maximum-size", "5000")
                .buildOrThrow();

        BaseJdbcConfig expected = new BaseJdbcConfig()
                .setConnectionUrl("jdbc:h2:mem:config")
                .setJdbcTypesMappedToVarchar("mytype, struct_type1")
                .setMetadataCacheTtl(new Duration(1, SECONDS))
                .setCacheMissing(true)
                .setCacheMaximumSize(5000);

        assertFullMapping(properties, expected);

        assertEquals(expected.getJdbcTypesMappedToVarchar(), ImmutableSet.of("mytype", "struct_type1"));
    }

    @Test
    public void testConnectionUrlIsValid()
    {
        assertThatThrownBy(() -> buildConfig(ImmutableMap.of("connection-url", "jdbc:")))
                .hasMessageContaining("must match the following regular expression: ^jdbc:[a-z0-9]+:(?s:.*)$");
        assertThatThrownBy(() -> buildConfig(ImmutableMap.of("connection-url", "jdbc:protocol")))
                .hasMessageContaining("must match the following regular expression: ^jdbc:[a-z0-9]+:(?s:.*)$");
        buildConfig(ImmutableMap.of("connection-url", "jdbc:protocol:uri"));
        buildConfig(ImmutableMap.of("connection-url", "jdbc:protocol:"));
    }

    @Test
    public void testCacheConfigValidation()
    {
        BaseJdbcConfig explicitCacheSize = new BaseJdbcConfig()
                .setMetadataCacheTtl(new Duration(1, SECONDS))
                .setCacheMaximumSize(5000);
        explicitCacheSize.validate();
        BaseJdbcConfig defaultCacheSize = new BaseJdbcConfig()
                .setMetadataCacheTtl(new Duration(1, SECONDS));
        defaultCacheSize.validate();
        assertThatThrownBy(() -> new BaseJdbcConfig()
                .setMetadataCacheTtl(ZERO)
                .setCacheMaximumSize(100000)
                .validate())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("metadata.cache-ttl must be set to a non-zero value when metadata.cache-maximum-size is set");
    }

    private static void buildConfig(Map<String, String> properties)
    {
        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        configurationFactory.build(BaseJdbcConfig.class);
    }
}
