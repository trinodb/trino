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
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static io.airlift.units.Duration.ZERO;
import static io.trino.plugin.jdbc.BaseJdbcConfig.DEFAULT_STATISTICS_CACHE_TTL;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestBaseJdbcConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(BaseJdbcConfig.class)
                .setConnectionUrl(null)
                .setJdbcTypesMappedToVarchar(ImmutableSet.of())
                .setMetadataCacheTtl(ZERO)
                .setSchemaNamesCacheTtl(null)
                .setTableNamesCacheTtl(null)
                .setStatisticsCacheTtl(DEFAULT_STATISTICS_CACHE_TTL)
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
                .put("metadata.schemas.cache-ttl", "2s")
                .put("metadata.tables.cache-ttl", "3s")
                .put("metadata.statistics.cache-ttl", "7s")
                .put("metadata.cache-missing", "true")
                .put("metadata.cache-maximum-size", "5000")
                .buildOrThrow();

        BaseJdbcConfig expected = new BaseJdbcConfig()
                .setConnectionUrl("jdbc:h2:mem:config")
                .setJdbcTypesMappedToVarchar(ImmutableSet.of("mytype", "struct_type1"))
                .setMetadataCacheTtl(new Duration(1, SECONDS))
                .setSchemaNamesCacheTtl(new Duration(2, SECONDS))
                .setTableNamesCacheTtl(new Duration(3, SECONDS))
                .setStatisticsCacheTtl(new Duration(7, SECONDS))
                .setCacheMissing(true)
                .setCacheMaximumSize(5000);

        assertFullMapping(properties, expected);

        assertThat(expected.getJdbcTypesMappedToVarchar()).containsOnly("mytype", "struct_type1");
    }

    @Test
    public void testStatsCacheTtl()
    {
        // enabled by default
        assertThat(new BaseJdbcConfig().getStatisticsCacheTtl()).isEqualTo(DEFAULT_STATISTICS_CACHE_TTL);

        // takes higher of the DEFAULT_STATISTICS_CACHE_TTL or metastoreTtl if not set explicitly
        assertThat(new BaseJdbcConfig()
                .setMetadataCacheTtl(new Duration(1, SECONDS))
                .getStatisticsCacheTtl()).isEqualTo(DEFAULT_STATISTICS_CACHE_TTL);
        assertThat(new BaseJdbcConfig()
                .setMetadataCacheTtl(new Duration(1111, DAYS))
                .getStatisticsCacheTtl()).isEqualTo(new Duration(1111, DAYS));

        // explicit configuration is honored
        assertThat(new BaseJdbcConfig()
                .setStatisticsCacheTtl(new Duration(135, MILLISECONDS))
                .setMetadataCacheTtl(new Duration(1111, DAYS))
                .getStatisticsCacheTtl()).isEqualTo(new Duration(135, MILLISECONDS));
    }

    @Test
    public void testConnectionUrlIsValid()
    {
        assertThatThrownBy(() -> buildConfig(ImmutableMap.of("connection-url", "jdbc:")))
                .hasMessageContaining("must match \"^jdbc:[a-z0-9]+:(?s:.*)$\"");
        assertThatThrownBy(() -> buildConfig(ImmutableMap.of("connection-url", "jdbc:protocol")))
                .hasMessageContaining("must match \"^jdbc:[a-z0-9]+:(?s:.*)$\"");
        buildConfig(ImmutableMap.of("connection-url", "jdbc:protocol:uri"));
        buildConfig(ImmutableMap.of("connection-url", "jdbc:protocol:"));
    }

    @Test
    public void testCacheConfigValidation()
    {
        assertValidates(new BaseJdbcConfig()
                .setConnectionUrl("jdbc:h2:mem:config")
                .setMetadataCacheTtl(new Duration(1, SECONDS))
                .setSchemaNamesCacheTtl(new Duration(2, SECONDS))
                .setTableNamesCacheTtl(new Duration(3, SECONDS))
                .setCacheMaximumSize(5000));

        assertValidates(new BaseJdbcConfig()
                .setConnectionUrl("jdbc:h2:mem:config")
                .setStatisticsCacheTtl(new Duration(7, SECONDS))
                .setCacheMaximumSize(5000));

        assertValidates(new BaseJdbcConfig()
                .setConnectionUrl("jdbc:h2:mem:config")
                .setMetadataCacheTtl(new Duration(1, SECONDS)));

        assertFailsValidation(
                new BaseJdbcConfig()
                        .setCacheMaximumSize(5000),
                "cacheMaximumSizeConsistent",
                "metadata.cache-ttl or metadata.statistics.cache-ttl must be set to a non-zero value when metadata.cache-maximum-size is set",
                AssertTrue.class);

        assertFailsValidation(
                new BaseJdbcConfig()
                        .setSchemaNamesCacheTtl(new Duration(1, SECONDS)),
                "schemaNamesCacheTtlConsistent",
                "metadata.schemas.cache-ttl must not be set when metadata.cache-ttl is not set",
                AssertTrue.class);

        assertFailsValidation(
                new BaseJdbcConfig()
                        .setTableNamesCacheTtl(new Duration(1, SECONDS)),
                "tableNamesCacheTtlConsistent",
                "metadata.tables.cache-ttl must not be set when metadata.cache-ttl is not set",
                AssertTrue.class);
    }

    private static void buildConfig(Map<String, String> properties)
    {
        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        configurationFactory.build(BaseJdbcConfig.class);
    }
}
