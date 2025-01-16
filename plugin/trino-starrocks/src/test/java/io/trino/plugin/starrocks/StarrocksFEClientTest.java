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
package io.trino.plugin.starrocks;

class StarrocksFEClientTest
{
    private static final String JDBC_URL = "jdbc:mysql://localhost:9030";
    private static final String Scanner_URL = "localhost:8030";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";

//    private StarrocksConfig getStarrocksConfig()
//    {
//        StarrocksConfig config = new StarrocksConfig();
//        config.setJdbcURL(JDBC_URL);
//        config.setScanURL(Scanner_URL);
//        config.setUsername(USERNAME);
//        config.setPassword(PASSWORD);
//        return config;
//    }
//
//    @Test
//    void testListTables()
//    {
//        StarrocksFEClient client = new StarrocksFEClient(getStarrocksConfig());
//        Optional<String> schemaName = Optional.of("quickstart");
//        List<String> tables = client.listTables(schemaName, TestingConnectorSession.SESSION);
//        System.out.println(tables);
//        assertNotNull(tables);
//    }
//
//    @Test
//    void testlistSchemaNames()
//    {
//        StarrocksFEClient client = new StarrocksFEClient(getStarrocksConfig());
//        List<String> schema = client.getSchemaNames(TestingConnectorSession.SESSION);
//        System.out.println(schema);
//        assertNotNull(schema);
//    }
//
//    @Test
//    void testGetTableHandle()
//    {
//        StarrocksFEClient client = new StarrocksFEClient(getStarrocksConfig());
//        ConnectorTableHandle tableHandle = client.getTableHandle(
//                TestingConnectorSession.SESSION,
//                new SchemaTableName("quickstart", "crashdata"));
//        assertNotNull(tableHandle);
//    }
//
//    @Test
//    void testGetTableMetadata()
//    {
//        StarrocksFEClient client = new StarrocksFEClient(getStarrocksConfig());
//        StarrocksTableHandle tableHandle = client.getTableHandle(
//                TestingConnectorSession.SESSION,
//                new SchemaTableName("quickstart", "crashdata"));
//        ConnectorTableMetadata tableMetadata = client.getTableMetaData(
//                TestingConnectorSession.SESSION,
//                tableHandle);
//        assertNotNull(tableMetadata);
//    }
//
//    @Test
//    void testGetColumnMetadata()
//    {
//        StarrocksFEClient client = new StarrocksFEClient(getStarrocksConfig());
//        StarrocksTableHandle tableHandle = client.getTableHandle(
//                TestingConnectorSession.SESSION,
//                new SchemaTableName("quickstart", "crashdata"));
//    }
}
