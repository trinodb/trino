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

import com.google.inject.ConfigurationException;
import io.airlift.configuration.ConfigurationFactory;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

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
                .setExcludedColumns(Set.of())
                .setTerminateOnInitializationFailure(true));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = Map.of(
                "mysql-event-listener.db.url", "jdbc:mysql://example.net:3306",
                "mysql-event-listener.excluded-columns", "plan,stage_info_json,operator_summaries_json",
                "mysql-event-listener.terminate-on-initialization-failure", "false");

        MysqlEventListenerConfig expected = new MysqlEventListenerConfig()
                .setUrl("jdbc:mysql://example.net:3306")
                .setExcludedColumns(Set.of("plan", "stage_info_json", "operator_summaries_json"))
                .setTerminateOnInitializationFailure(false);

        assertFullMapping(properties, expected);
    }

    @Test
    void testExcludedColumns()
    {
        MysqlEventListenerConfig config = new MysqlEventListenerConfig()
                .setExcludedColumns(Set.of(" PLAN ", "", "stage_info_json"));
        assertThat(config.getExcludedColumns()).containsExactlyInAnyOrder("plan", "stage_info_json");
    }

    @Test
    void testInvalidExcludedColumns()
    {
        Map<String, String> properties = Map.of(
                "mysql-event-listener.db.url", "jdbc:mysql://example.net:3306",
                "mysql-event-listener.excluded-columns", "cpu_time_millis");

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        assertThatThrownBy(() -> configurationFactory.build(MysqlEventListenerConfig.class))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Only supported MySQL event listener text columns can be excluded");
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
