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
package io.prestosql.plugin.salesforce.driver;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ForceDriverTest
{
    private ForceDriver driver;

    @Before
    public void setUp()
    {
        driver = new ForceDriver();
    }

    @Test
    public void testGetConnStringProperties()
            throws IOException
    {
        Properties actuals = driver.getConnStringProperties("jdbc:salesforce://prop1=val1;prop2=val2");

        assertEquals(2, actuals.size());
        assertEquals("val1", actuals.getProperty("prop1"));
        assertEquals("val2", actuals.getProperty("prop2"));
    }

    @Test
    public void testGetConnStringProperties_WhenNoValue()
            throws IOException
    {
        Properties actuals = driver.getConnStringProperties("jdbc:salesforce://prop1=val1; prop2; prop3 = val3");

        assertEquals(3, actuals.size());
        assertTrue(actuals.containsKey("prop2"));
        assertEquals("", actuals.getProperty("prop2"));
    }

    @Test
    public void testConnect_WhenWrongURL()
            throws SQLException
    {
        Connection connection = driver.connect("jdbc:mysql://localhost/test", new Properties());

        assertNull(connection);
    }

    @Test
    public void testGetConnStringProperties_StandartUrlFormat()
            throws IOException
    {
        Properties actuals = driver.getConnStringProperties("jdbc:salesforce://test@test.ru:aaaa!aaa@login.salesforce.ru");

        assertEquals(2, actuals.size());
        assertTrue(actuals.containsKey("user"));
        assertEquals("test@test.ru", actuals.getProperty("user"));
        assertEquals("aaaa!aaa", actuals.getProperty("password"));
    }
}
