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
import java.util.Optional;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestMysqlEventListenerConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(MysqlEventListenerConfig.class)
                .setUrl(null)
                .setUser(null)
                .setPassword(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = Map.of(
                "mysql-event-listener.db.url", "jdbc:mysql://example.net:3306",
                "mysql-event-listener.db.user", "user",
                "mysql-event-listener.db.password", "password");

        MysqlEventListenerConfig expected = new MysqlEventListenerConfig()
                .setUrl("jdbc:mysql://example.net:3306")
                .setUser("user")
                .setPassword("password");

        assertFullMapping(properties, expected);
    }

    @Test
    void testIsValidUrl()
    {
        assertThat(isValidUrl("jdbc:mysql://example.net:3306")).isTrue();
        assertThat(isValidUrl("jdbc:mysql://example.net:3306/")).isTrue();
        assertThat(isValidUrl("jdbc:mysql://example.net:3306/?user=trino&password=123456")).isTrue();
        assertThat(isValidUrl("jdbc:postgresql://example.net:3306/somedatabase")).isFalse();
    }

    @Test
    void testIsAuthenticationRedundant()
    {
        assertThatThrownBy(() -> isAuthenticationRedundant("jdbc:mysql://example.net:3306?user=trino", Optional.of("trino"), Optional.empty()))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> isAuthenticationRedundant("jdbc:mysql://example.net:3306?password=123456", Optional.empty(), Optional.of("123456")))
                .isInstanceOf(RuntimeException.class);
    }

    private static boolean isAuthenticationRedundant(String url, Optional<String> user, Optional<String> password)
    {
        MysqlEventListenerConfig config = new MysqlEventListenerConfig().setUrl(url);
        if (user.isPresent()) {
            config.setUser(user.get());
        }
        if (password.isPresent()) {
            config.setPassword(password.get());
        }
        return config.isValidUrl();
    }

    private static boolean isValidUrl(String url)
    {
        return new MysqlEventListenerConfig()
                .setUrl(url)
                .isValidUrl();
    }
}
