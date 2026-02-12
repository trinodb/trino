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
package io.trino.plugin.eventlistener.querylog;

import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.units.DataSize.Unit.KILOBYTE;

public class TestQueryLogEventListenerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(QueryLogEventListenerConfig.class)
                .setLogCreated(false)
                .setLogCompleted(false)
                .setLogExecuted(false)
                .setLogFilePath("querylog.log")
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
                Map.entry("querylog-event-listener.log-created", "true"),
                Map.entry("querylog-event-listener.log-completed", "true"),
                Map.entry("querylog-event-listener.log-executed", "true"),
                Map.entry("querylog-event-listener.log-file-path", "/var/log/trino/querylog.log"),
                Map.entry("querylog-event-listener.excluded-fields", "payload,user,sourceCode"),
                Map.entry("querylog-event-listener.max-field-size", "8KB"),
                Map.entry("querylog-event-listener.truncated-fields", "query,stageInfo"),
                Map.entry("querylog-event-listener.truncation-size-limit", "1KB"),
                Map.entry("querylog-event-listener.ignored-query-states", "RUNNING,QUEUED"),
                Map.entry("querylog-event-listener.ignored-update-types", "INSERT,UPDATE"),
                Map.entry("querylog-event-listener.ignored-query-types", "UTILITY"),
                Map.entry("querylog-event-listener.ignored-failure-types", "USER_ERROR"));

        assertFullMapping(properties, new QueryLogEventListenerConfig()
                .setLogCreated(true)
                .setLogCompleted(true)
                .setLogExecuted(true)
                .setLogFilePath("/var/log/trino/querylog.log")
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
                "querylog-event-listener.excluded-fields", "payload, , user");

        assertFullMapping(properties, new QueryLogEventListenerConfig()
                .setExcludedFields(Set.of("payload", "user")));
    }

    @Test
    public void testTruncatedFieldsFilteringBlankValues()
    {
        Map<String, String> properties = Map.of(
                "querylog-event-listener.truncated-fields", "query, , stageInfo");

        assertFullMapping(properties, new QueryLogEventListenerConfig()
                .setTruncatedFields(Set.of("query", "stageInfo")));
    }

    @Test
    public void testIgnoredQueryStatesFilteringBlankValues()
    {
        Map<String, String> properties = Map.of(
                "querylog-event-listener.ignored-query-states", "RUNNING, , QUEUED");

        assertFullMapping(properties, new QueryLogEventListenerConfig()
                .setIgnoredQueryStates(Set.of("RUNNING", "QUEUED")));
    }
}
