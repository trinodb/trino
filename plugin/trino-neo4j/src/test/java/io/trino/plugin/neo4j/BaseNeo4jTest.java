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

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.Optional;

public class BaseNeo4jTest
        extends AbstractTestQueryFramework
{
    private Optional<Iterable<TpchTable<?>>> tables;
    protected TestingNeo4jServer neo4jServer;

    public BaseNeo4jTest(Optional<Iterable<TpchTable<?>>> tables)
    {
        this.tables = tables;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        neo4jServer = closeAfterClass(new TestingNeo4jServer());
        return Neo4jQueryRunner.createNeo4jQueryRunner(neo4jServer, ImmutableMap.of(), ImmutableMap.of(), this.tables);
    }
}
