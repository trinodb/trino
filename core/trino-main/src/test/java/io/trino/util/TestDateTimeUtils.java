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
package io.trino.util;

import org.testng.annotations.Test;

import java.time.DateTimeException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestDateTimeUtils
{
    @Test
    public void testTryParseDateYyyyMmDd()
    {
        // valid dates
        int days = DateTimeUtils.tryParseDateYyyyMmDd("1970-01-01");
        assertEquals(0, days);
        days = DateTimeUtils.tryParseDateYyyyMmDd("1970-02-01");
        assertEquals(31, days);
        days = DateTimeUtils.tryParseDateYyyyMmDd("1969-12-01");
        assertEquals(-31, days);

        // format invalid
        days = DateTimeUtils.tryParseDateYyyyMmDd("1970-2-01");
        assertEquals(Integer.MIN_VALUE, days);
        days = DateTimeUtils.tryParseDateYyyyMmDd("1970-02-1");
        assertEquals(Integer.MIN_VALUE, days);
        days = DateTimeUtils.tryParseDateYyyyMmDd("1970/02/01");
        assertEquals(Integer.MIN_VALUE, days);
        days = DateTimeUtils.tryParseDateYyyyMmDd("Dec 24 2022");
        assertEquals(Integer.MIN_VALUE, days);

        // format ok, but illegal value
        assertThatThrownBy(() -> DateTimeUtils.tryParseDateYyyyMmDd("2022-02-29"))
                .isInstanceOf(DateTimeException.class)
                .hasMessage("Invalid date 'February 29' as '2022' is not a leap year");
    }
}
