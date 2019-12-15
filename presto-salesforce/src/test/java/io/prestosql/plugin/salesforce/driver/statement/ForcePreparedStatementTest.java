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
package io.prestosql.plugin.salesforce.driver.statement;

import org.junit.Test;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ForcePreparedStatementTest
{
    @Test
    public void testGetParamClass()
    {
        assertEquals(String.class, ForcePreparedStatement.getParamClass("test"));
        assertEquals(Long.class, ForcePreparedStatement.getParamClass(1L));
        assertEquals(Object.class, ForcePreparedStatement.getParamClass(new SimpleDateFormat()));
        assertNull(ForcePreparedStatement.getParamClass(null));
    }

    @Test
    public void testToSoqlStringParam()
    {
        assertEquals("'\\''", ForcePreparedStatement.toSoqlStringParam("'"));
        assertEquals("'\\\\'", ForcePreparedStatement.toSoqlStringParam("\\"));
        assertEquals("'\\';DELETE DATABASE \\\\a'", ForcePreparedStatement.toSoqlStringParam("';DELETE DATABASE \\a"));
    }

    @Test
    public void testConvertToSoqlParam()
    {
        assertEquals("123.45", ForcePreparedStatement.convertToSoqlParam(123.45));
        assertEquals("123.45", ForcePreparedStatement.convertToSoqlParam(123.45f));
        assertEquals("123", ForcePreparedStatement.convertToSoqlParam(123L));
        assertEquals("123.45", ForcePreparedStatement.convertToSoqlParam(new BigDecimal("123.45")));
        assertEquals("2017-03-06T12:34:56", ForcePreparedStatement.convertToSoqlParam(new GregorianCalendar(2017, 2, 6, 12, 34, 56).getTime()));
        assertEquals("'\\'test\\'\\\\'", ForcePreparedStatement.convertToSoqlParam("'test'\\"));
        assertEquals("NULL", ForcePreparedStatement.convertToSoqlParam(null));
    }

    @Test
    public void testAddParameter()
    {
        ForcePreparedStatement statement = new ForcePreparedStatement(null, "");
        statement.addParameter(1, "one");
        statement.addParameter(3, "two");

        List<Object> actuals = statement.getParameters();

        assertEquals(3, actuals.size());
        assertEquals("one", actuals.get(0));
        assertEquals("two", actuals.get(2));
        assertNull(actuals.get(1));
    }

    @Test
    public void testSetParams()
    {
        ForcePreparedStatement statement = new ForcePreparedStatement(null, "");
        String query = "SELECT Something FROM Anything WERE name = ? AND age > ?";
        statement.addParameter(1, "one");
        statement.addParameter(2, 123);

        String actual = statement.setParams(query);

        assertEquals("SELECT Something FROM Anything WERE name = 'one' AND age > 123", actual);
    }

    @Test
    public void testGetCacheMode()
    {
        ForcePreparedStatement statement = new ForcePreparedStatement(null, "");

        assertEquals(ForcePreparedStatement.CacheMode.SESSION, statement.getCacheMode("CACHE SESSION select name from Account"));
        assertEquals(ForcePreparedStatement.CacheMode.GLOBAL, statement.getCacheMode(" Cache global select name from Account"));
        assertEquals(ForcePreparedStatement.CacheMode.NO_CACHE, statement.getCacheMode("select name from Account"));
        assertEquals(ForcePreparedStatement.CacheMode.NO_CACHE, statement.getCacheMode(" Cache unknown select name from Account"));
    }

    @Test
    public void removeCacheHints()
    {
        ForcePreparedStatement statement = new ForcePreparedStatement(null, "");
        assertEquals("  select name from Account", statement.removeCacheHints(" Cache global select name from Account"));
    }
}
