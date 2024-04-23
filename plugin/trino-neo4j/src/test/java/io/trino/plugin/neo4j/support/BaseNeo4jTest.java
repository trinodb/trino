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

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

import java.util.Optional;

public class BaseNeo4jTest
        extends AbstractTestQueryFramework
{
    protected Neo4jTestingServer neo4jServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Optional<String> neo4jInstanceUrl = Optional.ofNullable(System.getenv("NEO4J_INSTANCE_URL"));
        Optional<String> neo4jUser = Optional.ofNullable(System.getenv("NEO4J_USER"));
        Optional<String> neo4jPassword = Optional.ofNullable(System.getenv("NEO4J_PASSWORD"));

        if (neo4jInstanceUrl.isPresent()) {
            this.neo4jServer = closeAfterClass(new Neo4JExternalTestingServer(
                    neo4jInstanceUrl.get(),
                    neo4jUser.orElse("neo4j"),
                    neo4jPassword.orElseThrow()));
        }
        else {
            this.neo4jServer = closeAfterClass(new Neo4JContainerTestingServer());
        }

        return Neo4jQueryRunner.createNeo4jQueryRunner(this.neo4jServer, ImmutableMap.of(), ImmutableMap.of());
    }
}
