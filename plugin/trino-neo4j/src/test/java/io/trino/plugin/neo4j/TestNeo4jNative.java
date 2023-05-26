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

import org.testng.annotations.Test;

import java.util.Optional;

public class TestNeo4jNative
        extends BaseNeo4jTest
{
    public TestNeo4jNative()
    {
        super(Optional.empty());
    }

    @Test
    public void testCreateNode()
    {
        String query = buildPassThroughQuery("CREATE (m:Movie {title: ''Star Wars'', released: 1977})" +
                "return m.title");
        assertQueryFails(query, ".*Writing in read access mode not allowed.*");
    }

    @Test
    public void testDeleteNode()
    {
        String query = buildPassThroughQuery("MATCH (m:movie {title:''Bourne Identity''}) delete m ");
        assertQueryFails(query, ".*Not supported.*");
    }

    @Test
    public void testMatch()
    {
        String query = buildPassThroughQuery(
                "MATCH (m:Movie {title:''The Matrix''})" +
                        "-[rel:ACTED_IN]-(p:Person) " +
                        "RETURN p.name");
        assertQuery(query, "VALUES ('Keanu Reeves'), ('Carrie-Anne Moss'), ('Laurence Fishburne'), ('Hugo Weaving')");
    }

    @Test
    public void testOptionalMatch()
    {
        String query = buildPassThroughQuery(
                "MATCH (m:Movie {title:''Bourne Identity''}) " +
                        "OPTIONAL MATCH (m)-[rel:ACTED_IN]-(p:Person) " +
                        "RETURN m.title, p.name");
        assertQuery(query, "VALUES ('Bourne Identity', null)");
    }

    @Test
    public void testWithClause()
    {
        String query = buildPassThroughQuery(
                "MATCH (m:Movie {title:''Mission Impossible''})" +
                        "-[rel:ACTED_IN]-(p:Person) " +
                        " with p where p.name starts with ''Tom'' " +
                        "RETURN p.name");
        assertQuery(query, "VALUES ('Tom Cruise')");
    }

    @Test
    public void testUnwindClause()
    {
        String query = buildPassThroughQuery(
                "UNWIND [1, 2] AS x " +
                        " RETURN x, x+1 AS y");
        assertQuery(query, "VALUES (1, 2), (2, 3)");
    }

    @Test
    public void testWhereClause()
    {
        String query = buildPassThroughQuery(
                "MATCH (m:Movie) where m.released = 1999 " +
                        " return m.title");
        assertQuery(query, "VALUES ('The Matrix')");
    }

    @Test
    public void testCallSubquery()
    {
        String query = buildPassThroughQuery(
                "call {MATCH (m:Movie) return m.title order by m.released} " +
                        " return `m.title`");
        assertQuery(query, "VALUES ('Mission Impossible'), ('The Matrix'), ('Bourne Identity')");
    }

    @Test
    public void testCallProcedure()
    {
        // unfortunately have to repeat the columns to project twice. Once in 'yield' and then again in 'return'
        String query = buildPassThroughQuery(
                "call db.labels() yield label " +
                        " return label");
        assertQuery(query, "VALUES ('Movie'), ('Person')");
    }

    @Test
    public void testUnion()
    {
        String query = buildPassThroughQuery(
                "MATCH (n:Person {name: ''Keanu Reeves''}) " +
                        " return n.name" +
                        " union all " +
                        " MATCH (n:Person {name: ''Tom Cruise''}) " +
                        " return n.name ");
        assertQuery(query, "VALUES ('Keanu Reeves'), ('Tom Cruise')");
    }

    @Test
    public void testUse()
    {
        String query = buildPassThroughQuery(
                "use neo4j " +
                        " match (n:Movie) " +
                        " return count(*)");
        assertQuery(query, "VALUES (3)");
    }

    @Test
    public void testApocProcedure()
    {
        String query = buildPassThroughQuery(
                "RETURN  " +
                        " apoc.temporal.format( ''2023-01-01'', ''YYYY-MM-dd'') " +
                        " AS output");
        assertQuery(query, "VALUES (date '2023-01-01')");
    }

    private String buildPassThroughQuery(String nativeQuery)
    {
        return String.format("select * from TABLE(neo4j.system.query(query => '%s'))", nativeQuery);
    }
}
