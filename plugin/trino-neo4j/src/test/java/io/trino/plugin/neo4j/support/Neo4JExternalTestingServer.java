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
package io.trino.plugin.neo4j.support;

public class Neo4JExternalTestingServer
        implements Neo4jTestingServer
{
    private final String uri;
    private final String user;
    private final String password;

    public Neo4JExternalTestingServer(String uri, String user, String password)
    {
        this.uri = uri;
        this.user = user;
        this.password = password;
    }

    @Override
    public String getUri()
    {
        return this.uri;
    }

    @Override
    public String getUsername()
    {
        return this.user;
    }

    @Override
    public String getPassword()
    {
        return this.password;
    }

    public static void main(String[] args)
            throws Exception
    {
        try (Neo4JExternalTestingServer server = new Neo4JExternalTestingServer("bolt://127.0.0.1:7687", "neo4j", "passpass")) {
            System.out.println("neo4j.uri: " + server.getUri());

            server.withSession(f -> System.out.println(f.run("return 42")));
        }
    }

    @Override
    public void close()
            throws Exception
    {
    }
}
