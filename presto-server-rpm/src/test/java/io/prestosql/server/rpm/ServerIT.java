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
package io.prestosql.server.rpm;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import static java.sql.DriverManager.getConnection;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public class ServerIT
{
    @Parameters("presto.port")
    @Test
    public void testServer(int port)
            throws Exception
    {
        String url = "jdbc:presto://localhost:" + port;
        try (Connection connection = getConnection(url, "test", null);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SHOW CATALOGS")) {
            Set<String> catalogs = new HashSet<>();
            while (rs.next()) {
                catalogs.add(rs.getString(1));
            }
            assertEquals(catalogs, new HashSet<>(asList("system", "hive")));
        }
    }
}
