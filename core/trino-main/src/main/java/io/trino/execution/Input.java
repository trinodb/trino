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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.CatalogVersion;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record Input(
        Optional<String> connectorName,
        String catalogName,
        CatalogVersion catalogVersion,
        String schema,
        String table,
        Optional<Object> connectorInfo,
        List<Column> columns,
        PlanFragmentId fragmentId,
        PlanNodeId planNodeId)
{
    public Input
    {
        requireNonNull(connectorName, "connectorName is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(catalogVersion, "catalogVersion is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(table, "table is null");
        requireNonNull(connectorInfo, "connectorInfo is null");
        columns = ImmutableList.copyOf(columns);
        requireNonNull(fragmentId, "fragmentId is null");
        requireNonNull(planNodeId, "planNodeId is null");
    }
}
