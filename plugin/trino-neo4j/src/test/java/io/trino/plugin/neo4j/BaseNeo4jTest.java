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
package io.trino.plugin.neo4j;

import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseNeo4jTest
{
    protected static TestingNeo4jServer neo4jServer;
    protected static QueryRunner queryRunner;

    @BeforeClass
    public static void setupNeo4jTests() throws Exception
    {
        neo4jServer = new TestingNeo4jServer();
//        neo4jServer = getExistingServer();
        queryRunner = Neo4jQueryRunner.createDefaultQueryRunner(neo4jServer);
    }

    @AfterClass
    public static void teardownNeo4jTests()
    {
        queryRunner.close();
        neo4jServer.close();
    }

    /**
     * Use an existing neo4j server
     * @return
     */
    private TestingNeo4jServer getExistingServer()
    {
        TestingNeo4jServer testingNeo4jServer = mock(TestingNeo4jServer.class);
        when(testingNeo4jServer.getJdbcUrl(Optional.empty())).thenReturn("jdbc:neo4j:bolt://localhost:7687");
        when(testingNeo4jServer.getUsername()).thenReturn("neo4j");
        when(testingNeo4jServer.getPassword()).thenReturn("password");
        return testingNeo4jServer;
    }
}
