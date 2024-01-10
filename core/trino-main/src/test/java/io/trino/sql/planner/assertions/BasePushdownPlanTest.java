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
package io.trino.sql.planner.assertions;

import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;

import java.util.Map;
import java.util.Optional;

/**
 * Extension of {@link BasePlanTest} that provides connector metadata methods
 */
public abstract class BasePushdownPlanTest
        extends BasePlanTest
{
    protected Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName objectName)
    {
        return getQueryRunner().inTransaction(session, transactionSession -> getQueryRunner().getMetadata().getTableHandle(transactionSession, objectName));
    }

    protected Map<String, ColumnHandle> getColumnHandles(Session session, QualifiedObjectName tableName)
    {
        return getQueryRunner().inTransaction(session, transactionSession -> {
            Optional<TableHandle> table = getQueryRunner().getMetadata().getTableHandle(transactionSession, tableName);
            return getQueryRunner().getMetadata().getColumnHandles(transactionSession, table.get());
        });
    }
}
