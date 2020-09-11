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
package io.prestosql.sql.planner.planprinter;

import io.prestosql.Session;
import io.prestosql.execution.TableInfo;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.metadata.TableProperties;
import io.prestosql.sql.planner.plan.TableScanNode;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class TableInfoSupplier
        implements Function<TableScanNode, TableInfo>
{
    private final Metadata metadata;
    private final Session session;

    public TableInfoSupplier(Metadata metadata, Session session)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public TableInfo apply(TableScanNode node)
    {
        TableMetadata tableMetadata = metadata.getTableMetadata(session, node.getTable());
        TableProperties tableProperties = metadata.getTableProperties(session, node.getTable());
        return new TableInfo(tableMetadata.getQualifiedName(), tableProperties.getPredicate());
    }
}
