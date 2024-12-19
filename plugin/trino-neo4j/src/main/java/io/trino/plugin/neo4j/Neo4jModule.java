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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.plugin.neo4j.ptf.Query;
import io.trino.plugin.neo4j.serialization.NodeSerializer;
import io.trino.plugin.neo4j.serialization.PointSerializer;
import io.trino.plugin.neo4j.serialization.RelationshipSerializer;
import io.trino.spi.function.table.ConnectorTableFunction;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.Relationship;
import org.neo4j.driver.types.TypeSystem;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;

public class Neo4jModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(Neo4jConnector.class).in(Scopes.SINGLETON);
        binder.bind(Neo4jMetadata.class).in(Scopes.SINGLETON);
        binder.bind(Neo4jClient.class).in(Scopes.SINGLETON);
        binder.bind(Neo4jSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(Neo4jRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(Neo4jTypeManager.class).in(Scopes.SINGLETON);
        binder.bind(TypeSystem.class).toProvider(TypeSystem::getDefault).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(Neo4jConnectorConfig.class);

        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);

        jsonBinder(binder).addSerializerBinding(Node.class).to(NodeSerializer.class);
        jsonBinder(binder).addSerializerBinding(Relationship.class).to(RelationshipSerializer.class);
        jsonBinder(binder).addSerializerBinding(Point.class).to(PointSerializer.class);
    }
}
