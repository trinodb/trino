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
package io.trino.plugin.eventlistener.logger;

import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.units.DataSize.Unit.KILOBYTE;

public class TestLoggerEventListenerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(LoggerEventListenerConfig.class)
                .setLogCreated(false)
                .setLogCompleted(false)
                .setLogFilePath("logger.log")
                .setExcludedFields(Set.of())
                .setMaxFieldSize(DataSize.of(4, KILOBYTE))
                .setTruncatedFields(Set.of())
                .setTruncationSizeLimit(DataSize.of(2, KILOBYTE))
                .setIgnoredQueryStates(Set.of())
                .setIgnoredUpdateTypes(Set.of())
                .setIgnoredQueryTypes(Set.of())
                .setIgnoredFailureTypes(Set.of()));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = Map.ofEntries(
                Map.entry("logger-event-listener.log-created", "true"),
                Map.entry("logger-event-listener.log-completed", "true"),
                Map.entry("logger-event-listener.log-file-path", "/var/log/trino/logger.log"),
                Map.entry("logger-event-listener.excluded-fields", "payload,user,sourceCode"),
                Map.entry("logger-event-listener.max-field-size", "8KB"),
                Map.entry("logger-event-listener.truncated-fields", "query,stageInfo"),
                Map.entry("logger-event-listener.truncation-size-limit", "1KB"),
                Map.entry("logger-event-listener.ignored-query-states", "RUNNING,QUEUED"),
                Map.entry("logger-event-listener.ignored-update-types", "INSERT,UPDATE"),
                Map.entry("logger-event-listener.ignored-query-types", "UTILITY"),
                Map.entry("logger-event-listener.ignored-failure-types", "USER_ERROR"));

        assertFullMapping(properties, new LoggerEventListenerConfig()
                .setLogCreated(true)
                .setLogCompleted(true)
                .setLogFilePath("/var/log/trino/logger.log")
                .setExcludedFields(Set.of("payload", "user", "sourceCode"))
                .setMaxFieldSize(DataSize.of(8, KILOBYTE))
                .setTruncatedFields(Set.of("query", "stageInfo"))
                .setTruncationSizeLimit(DataSize.of(1, KILOBYTE))
                .setIgnoredQueryStates(Set.of("RUNNING", "QUEUED"))
                .setIgnoredUpdateTypes(Set.of("INSERT", "UPDATE"))
                .setIgnoredQueryTypes(Set.of("UTILITY"))
                .setIgnoredFailureTypes(Set.of("USER_ERROR")));
    }

    @Test
    public void testExcludedFieldsFilteringBlankValues()
    {
        Map<String, String> properties = Map.of(
                "logger-event-listener.excluded-fields", "payload, , user");

        assertFullMapping(properties, new LoggerEventListenerConfig()
                .setExcludedFields(Set.of("payload", "user")));
    }

    @Test
    public void testTruncatedFieldsFilteringBlankValues()
    {
        Map<String, String> properties = Map.of(
                "logger-event-listener.truncated-fields", "query, , stageInfo");

        assertFullMapping(properties, new LoggerEventListenerConfig()
                .setTruncatedFields(Set.of("query", "stageInfo")));
    }

    @Test
    public void testIgnoredQueryStatesFilteringBlankValues()
    {
        Map<String, String> properties = Map.of(
                "logger-event-listener.ignored-query-states", "RUNNING, , QUEUED");

        assertFullMapping(properties, new LoggerEventListenerConfig()
                .setIgnoredQueryStates(Set.of("RUNNING", "QUEUED")));
    }
}
