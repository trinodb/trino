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
package io.trino.plugin.mariadb;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMariaDbJdbcConfig
{
    @Test
    public void testIsUrlValid()
    {
        assertTrue(isUrlValid("jdbc:mariadb://example.net:3306"));
        assertTrue(isUrlValid("jdbc:mariadb://example.net:3306/"));
        assertFalse(isUrlValid("jdbc:notmariadb://example.net:3306"));
        assertFalse(isUrlValid("jdbc:notmariadb://example.net:3306/"));
    }

    private static boolean isUrlValid(String url)
    {
        MariaDbJdbcConfig config = new MariaDbJdbcConfig();
        config.setConnectionUrl(url);
        return config.isUrlValid();
    }

    @Test
    public void testIsUrlWithoutDatabase()
    {
        assertTrue(isUrlWithoutDatabase("jdbc:mariadb://example.net:3306"));
        assertTrue(isUrlWithoutDatabase("jdbc:mariadb://example.net:3306/"));
        assertFalse(isUrlWithoutDatabase("jdbc:mariadb://example.net:3306/somedatabase"));
    }

    private static boolean isUrlWithoutDatabase(String url)
    {
        MariaDbJdbcConfig config = new MariaDbJdbcConfig();
        config.setConnectionUrl(url);
        return config.isUrlWithoutDatabase();
    }
}
