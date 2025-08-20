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
package io.trino.plugin.eventlistener.mysql;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;

final class TestMysqlEventListenerConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(MysqlEventListenerConfig.class)
                .setUrl(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = Map.of(
                "mysql-event-listener.db.url", "jdbc:mysql://example.net:3306");

        MysqlEventListenerConfig expected = new MysqlEventListenerConfig()
                .setUrl("jdbc:mysql://example.net:3306");

        assertFullMapping(properties, expected);
    }

    @Test
    void testIsValidUrl()
    {
        assertThat(isValidUrl("jdbc:mysql://example.net:3306")).isTrue();
        assertThat(isValidUrl("jdbc:mysql://example.net:3306/")).isTrue();
        assertThat(isValidUrl("jdbc:postgresql://example.net:3306/somedatabase")).isFalse();
    }

    private static boolean isValidUrl(String url)
    {
        return new MysqlEventListenerConfig()
                .setUrl(url)
                .isValidUrl();
    }
}
