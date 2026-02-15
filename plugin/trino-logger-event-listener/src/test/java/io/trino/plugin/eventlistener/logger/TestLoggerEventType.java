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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLoggerEventType
{
    @Test
    public void testEventTypeValues()
    {
        assertThat(LoggerEventType.QUERY_CREATED).isNotNull();
        assertThat(LoggerEventType.QUERY_COMPLETED).isNotNull();
    }

    @Test
    public void testEventTypeNames()
    {
        assertThat(LoggerEventType.QUERY_CREATED).hasToString("QUERY_CREATED");
        assertThat(LoggerEventType.QUERY_COMPLETED).hasToString("QUERY_COMPLETED");
    }

    @Test
    public void testEventTypeCount()
    {
        LoggerEventType[] values = LoggerEventType.values();
        assertThat(values).hasSize(2);
    }

    @Test
    public void testEventTypeValueOf()
    {
        assertThat(LoggerEventType.valueOf("QUERY_CREATED")).isEqualTo(LoggerEventType.QUERY_CREATED);
        assertThat(LoggerEventType.valueOf("QUERY_COMPLETED")).isEqualTo(LoggerEventType.QUERY_COMPLETED);
    }
}
