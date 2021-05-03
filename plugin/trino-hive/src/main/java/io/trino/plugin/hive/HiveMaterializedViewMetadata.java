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
package io.trino.plugin.hive;

import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;

public interface HiveMaterializedViewMetadata
{
    List<PropertyMetadata<?>> getMaterializedViewProperties();

    void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting);

    void dropMaterializedView(ConnectorSession session, SchemaTableName viewName);

    Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName);

    MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName name);
}
