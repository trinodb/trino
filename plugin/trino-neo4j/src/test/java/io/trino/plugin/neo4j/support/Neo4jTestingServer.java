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

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import java.util.function.Consumer;

public interface Neo4jTestingServer
        extends AutoCloseable
{
    String getUri();

    String getUsername();

    String getPassword();

    default void withSession(Consumer<Session> f)
    {
        try (Driver driver = GraphDatabase.driver(getUri(), AuthTokens.basic(getUsername(), getPassword()));
                Session session = driver.session()) {
            f.accept(session);
        }
    }
}
