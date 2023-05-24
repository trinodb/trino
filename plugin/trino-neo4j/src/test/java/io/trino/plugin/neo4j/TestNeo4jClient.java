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

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcQueryRelationHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.StaticCredentialProvider;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.DefaultIdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestNeo4jClient
        extends AbstractTestQueryFramework
{
    private Neo4jClient neo4jClient;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingNeo4jServer neo4jServer = closeAfterClass(new TestingNeo4jServer());
        setUpClass(neo4jServer);
        return Neo4jQueryRunner.createDefaultQueryRunner(neo4jServer);
    }

    private void setUpClass(TestingNeo4jServer neo4jServer) throws Exception
    {
        BaseJdbcConfig jdbcConfig = new BaseJdbcConfig();
        jdbcConfig.setConnectionUrl(neo4jServer.getJdbcUrl(Optional.empty()));
        CredentialProvider credentialProvider = new StaticCredentialProvider(Optional.of(neo4jServer.getUsername()), Optional.of(neo4jServer.getPassword()));
        ConnectionFactory connectionFactory = Neo4jClientModule.createConnectionFactory(jdbcConfig, credentialProvider);

        neo4jClient = new Neo4jClient(
                jdbcConfig,
                connectionFactory,
                new DefaultQueryBuilder(),
                new DefaultIdentifierMapping(),
                TESTING_TYPE_MANAGER, RemoteQueryModifier.NONE);
    }

    @Test
    public void testGetTableHandle()
    {
        // valid query with 4 columns
        PreparedQuery query = new PreparedQuery("MATCH (m:Movie)-[rel:ACTED_IN]-(p:Person) RETURN m.title as title, p.name, Type(rel) as relationship, rel.roles", ImmutableList.of());
        JdbcTableHandle tableHandle = neo4jClient.getTableHandle(getQueryRunner().getDefaultSession().toConnectorSession(), query);
        assertEquals(tableHandle.getRelationHandle().getClass(), JdbcQueryRelationHandle.class);
        assertEquals(tableHandle.getColumns().get(), ImmutableList.of(
                new JdbcColumnHandle("title", JDBC_VARCHAR, VARCHAR),
                new JdbcColumnHandle("p.name", JDBC_VARCHAR, VARCHAR),
                new JdbcColumnHandle("relationship", JDBC_VARCHAR, VARCHAR),
                new JdbcColumnHandle("rel.roles", JDBC_VARCHAR, VARCHAR)));
        Cache<PreparedQuery, Neo4jResultSetInfo> cachedResultSetInfo = neo4jClient.getCachedResultSetInfo();
        assertTrue(cachedResultSetInfo.getIfPresent(query).isHasResultSet());
        assertNotNull(cachedResultSetInfo.getIfPresent(query).getMetadata());
    }

    @Test
    public void testGetTableHandleUnsupportedQuery()
    {
        Cache<PreparedQuery, Neo4jResultSetInfo> cachedResultSetInfo = neo4jClient.getCachedResultSetInfo();
        List<String> queries = ImmutableList.of(
                "CREATE (Matt:Person {name:'Matt Damon', born:1970})",  // DML Query
                "MATCH (m:Movie)");   // Doesn't return results
        for (String query : queries) {
            PreparedQuery preparedQuery = new PreparedQuery(query, ImmutableList.of());
            assertThatThrownBy(() -> neo4jClient.getTableHandle(getQueryRunner().getDefaultSession().toConnectorSession(), preparedQuery))
                    .isInstanceOf(TrinoException.class).hasMessageStartingWith("Not supported");
            assertFalse(cachedResultSetInfo.getIfPresent(preparedQuery).isHasResultSet());
            assertNull(cachedResultSetInfo.getIfPresent(preparedQuery).getMetadata());
        }
    }

    @Test
    public void testGetTableHandleQueryWithNoResults()
    {
        // Director node does not exist, so NULL types is returned from Neo4J which is returned as VARCHAR
        PreparedQuery invalidQuery = new PreparedQuery("MATCH (m:Director) return m", ImmutableList.of());
        JdbcTableHandle jdbcTableHandle = neo4jClient.getTableHandle(getQueryRunner().getDefaultSession().toConnectorSession(), invalidQuery);
        assertEquals(jdbcTableHandle.getColumns().get().get(0),
                new JdbcColumnHandle("m", JDBC_VARCHAR, VARCHAR));
    }
}
