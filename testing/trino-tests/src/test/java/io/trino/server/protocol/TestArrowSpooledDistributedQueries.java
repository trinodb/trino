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
package io.trino.server.protocol;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestArrowSpooledDistributedQueries
        extends AbstractSpooledQueryDataDistributedQueries
{
    @Override
    protected Map<String, String> spoolingConfig()
    {
        return ImmutableMap.of("protocol.spooling.encoding.arrow+zstd.enabled", "true");
    }

    @Test
    @Override
    public void testTimestampWithTimeZoneLiterals()
    {
        // TODO: Arrow serialization by design converts all timestamps with time zone to UTC
        // The original time zone information is not preserved in the Arrow format
        assertThatThrownBy(super::testTimestampWithTimeZoneLiterals)
                .hasMessageContaining("expected: 1960-01-22T03:04:05+06:00")
                .hasMessageContaining("but was: 1960-01-21T21:04:05Z");
    }

    @Test
    @Override
    public void testTimeWithTimeZoneLiterals()
    {
        // TODO: Arrow serialization by design converts all time with time zone to UTC
        // The original time zone information is not preserved in the Arrow format
        assertThatThrownBy(super::testTimeWithTimeZoneLiterals)
                .hasMessageContaining("expected: 03:04:05+06:00")
                .hasMessageContaining("but was: 21:04:05Z");
    }

    @Test
    @Override
    public void testIn()
    {
        // TODO: Arrow serialization by design converts all timestamps with time zone to UTC
        // The original time zone information is not preserved in the Arrow format
        assertThatThrownBy(super::testIn)
                .hasMessageContaining("1970-01-01T08:01+08:00")
                .hasMessageContaining("1970-01-01T00:01Z");
    }

    @Test
    @Override
    public void testAtTimeZone()
    {
        // TODO: Arrow serialization by design converts all timestamps with time zone to UTC
        // The original time zone information is not preserved in the Arrow format
        assertThatThrownBy(super::testAtTimeZone)
                .hasMessageContaining("2012-10-30T18:09+07:09")
                .hasMessageContaining("2012-10-30T11:00Z");
    }

    @Override
    protected String encoding()
    {
        return "arrow+zstd";
    }
}
