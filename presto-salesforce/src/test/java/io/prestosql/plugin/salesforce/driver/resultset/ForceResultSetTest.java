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
package io.prestosql.plugin.salesforce.driver.resultset;

import io.prestosql.plugin.salesforce.driver.metadata.ColumnMap;
import org.junit.Before;
import org.junit.Test;

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class ForceResultSetTest
{
    private ForceResultSet forceResultSet;

    @Before
    public void setUp()
    {
        ColumnMap<String, Object> columnMap = new ColumnMap<>();
        forceResultSet = new ForceResultSet(Collections.singletonList(columnMap));
    }

    @Test
    public void testParseDate()
    {
        Date actual = forceResultSet.parseDate("2017-06-23");

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(actual);

        assertEquals(2017, calendar.get(Calendar.YEAR));
        assertEquals(Calendar.JUNE, calendar.get(Calendar.MONTH));
        assertEquals(23, calendar.get(Calendar.DAY_OF_MONTH));
    }
}
