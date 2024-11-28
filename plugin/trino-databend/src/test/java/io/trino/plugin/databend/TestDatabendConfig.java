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
package io.trino.plugin.databend;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDatabendConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DatabendConfig.class)
                .setConnectionTimeout(Duration.valueOf("60s"))
                .setPresignedUrlDisabled(false)
                .setCacheMaximumSize(10000)
                .setCacheMissing(false)
                .setConnectionUrl(null)
                .setJdbcTypesMappedToVarchar(new HashSet<>())
                .setMetadataCacheTtl(Duration.ZERO)
                .setSchemaNamesCacheTtl(Duration.ZERO)
                .setStatisticsCacheTtl(Duration.ZERO)
                .setTableNamesCacheTtl(Duration.ZERO));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("databend.connection-timeout", Duration.valueOf("60s").toString())
                .put("databend.presigned-url-disabled", "true")
                .buildOrThrow();

        DatabendConfig expected = new DatabendConfig()
                .setConnectionTimeout(Duration.valueOf("60s"))
                .setPresignedUrlDisabled(true);

        assertThat(expected.getConnectionTimeout().toString())
                .isEqualTo(properties.get("databend.connection-timeout"));
        assertThat(expected.getPresignedUrlDisabled().toString())
                .isEqualTo(properties.get("databend.presigned-url-disabled"));
    }
}
