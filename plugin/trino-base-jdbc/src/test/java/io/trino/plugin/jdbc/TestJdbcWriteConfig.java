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
import io.airlift.configuration.ConfigurationFactory;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.plugin.jdbc.JdbcWriteConfig.MAX_ALLOWED_WRITE_BATCH_SIZE;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJdbcWriteConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(JdbcWriteConfig.class)
                .setWriteBatchSize(1000)
                .setNonTransactionalInsert(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("write.batch-size", "24")
                .put("insert.non-transactional-insert.enabled", "true")
                .buildOrThrow();

        JdbcWriteConfig expected = new JdbcWriteConfig()
                .setWriteBatchSize(24)
                .setNonTransactionalInsert(true);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testWriteBatchSizeValidation()
    {
        assertThatThrownBy(() -> makeConfig(ImmutableMap.of("write.batch-size", "-42")))
                .hasMessageContaining("write.batch-size: must be greater than or equal to 1");

        assertThatThrownBy(() -> makeConfig(ImmutableMap.of("write.batch-size", "0")))
                .hasMessageContaining("write.batch-size: must be greater than or equal to 1");

        assertThatThrownBy(() -> makeConfig(ImmutableMap.of("write.batch-size", String.valueOf(MAX_ALLOWED_WRITE_BATCH_SIZE + 1))))
                .hasMessageContaining("write.batch-size: must be less than or equal to");

        assertThatCode(() -> makeConfig(ImmutableMap.of("write.batch-size", "1")))
                .doesNotThrowAnyException();

        assertThatCode(() -> makeConfig(ImmutableMap.of("write.batch-size", "42")))
                .doesNotThrowAnyException();
    }

    private static JdbcWriteConfig makeConfig(Map<String, String> props)
    {
        return new ConfigurationFactory(props).build(JdbcWriteConfig.class);
    }
}
