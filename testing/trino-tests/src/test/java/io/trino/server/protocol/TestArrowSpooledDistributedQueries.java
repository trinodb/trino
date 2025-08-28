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
        assertThatThrownBy(super::testTimestampWithTimeZoneLiterals)
                .hasMessageContaining("Output columns [OutputColumn[sourcePageChannel=0, columnName=_col0, type=timestamp(0) with time zone]] are not supported for spooling encoding '%s'".formatted(encoding()));
    }

    @Test
    @Override
    public void testSelectLargeInterval()
    {
        assertThatThrownBy(super::testSelectLargeInterval)
                .hasMessageContaining("Output columns [OutputColumn[sourcePageChannel=0, columnName=_col0, type=interval year to month]] are not supported for spooling encoding '%s'".formatted(encoding()));
    }

    @Test
    @Override
    public void testTimeLiterals()
    {
        assertThatThrownBy(super::testTimeLiterals)
                .hasMessageContaining("class java.lang.Integer cannot be cast to class java.lang.String");
    }

    @Test
    @Override
    public void testTimestampLiterals()
    {
        assertThatThrownBy(super::testTimestampLiterals)
                .hasMessageContaining("expected: 1960-01-22T03:04:05.123 (java.time.LocalDateTime)");
    }

    @Test
    @Override
    public void testIn()
    {
        assertThatThrownBy(super::testIn)
                .hasMessageContaining("Output columns [OutputColumn[sourcePageChannel=0, columnName=x, type=timestamp(0) with time zone]] are not supported for spooling encoding '%s'".formatted(encoding()));
    }

    @Test
    @Override
    public void testTimeWithTimeZoneLiterals()
    {
        assertThatThrownBy(super::testTimeWithTimeZoneLiterals)
                .hasMessageContaining("Output columns [OutputColumn[sourcePageChannel=0, columnName=_col0, type=time(3) with time zone]] are not supported for spooling encoding '%s'".formatted(encoding()));
    }

    @Test
    @Override
    public void testAtTimeZone()
    {
        assertThatThrownBy(super::testAtTimeZone)
                .hasMessageContaining("Output columns [OutputColumn[sourcePageChannel=0, columnName=_col0, type=timestamp(0) with time zone]] are not supported for spooling encoding '%s'".formatted(encoding()));
    }

    @Test
    @Override
    public void testTransactionsTable()
    {
        assertThatThrownBy(super::testTransactionsTable)
                .hasMessageContaining("Output columns [OutputColumn[sourcePageChannel=4, columnName=create_time, type=timestamp(3) with time zone]] are not supported for spooling encoding '%s'".formatted(encoding()));
    }

    @Test
    @Override
    public void testValuesWithTimestamp()
    {
        assertThatThrownBy(super::testValuesWithTimestamp)
                .hasMessageContaining("Output columns [OutputColumn[sourcePageChannel=0, columnName=_col0, type=timestamp(3) with time zone], OutputColumn[sourcePageChannel=1, columnName=_col1, type=timestamp(3) with time zone]] are not supported for spooling encoding '%s'".formatted(encoding()));
    }

    @Override
    protected String encoding()
    {
        return "arrow+zstd";
    }
}
