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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryLogEventType
{
    @Test
    public void testEventTypeValues()
    {
        assertThat(QueryLogEventType.QUERY_CREATED).isNotNull();
        assertThat(QueryLogEventType.QUERY_COMPLETED).isNotNull();
        assertThat(QueryLogEventType.QUERY_EXECUTED).isNotNull();
    }

    @Test
    public void testEventTypeNames()
    {
        assertThat(QueryLogEventType.QUERY_CREATED).hasToString("QUERY_CREATED");
        assertThat(QueryLogEventType.QUERY_COMPLETED).hasToString("QUERY_COMPLETED");
        assertThat(QueryLogEventType.QUERY_EXECUTED).hasToString("QUERY_EXECUTED");
    }

    @Test
    public void testEventTypeCount()
    {
        QueryLogEventType[] values = QueryLogEventType.values();
        assertThat(values).hasSize(3);
    }

    @Test
    public void testEventTypeValueOf()
    {
        assertThat(QueryLogEventType.valueOf("QUERY_CREATED")).isEqualTo(QueryLogEventType.QUERY_CREATED);
        assertThat(QueryLogEventType.valueOf("QUERY_COMPLETED")).isEqualTo(QueryLogEventType.QUERY_COMPLETED);
        assertThat(QueryLogEventType.valueOf("QUERY_EXECUTED")).isEqualTo(QueryLogEventType.QUERY_EXECUTED);
    }
}
