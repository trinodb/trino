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
package io.prestosql.connector.system.jdbc;

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;

import static io.prestosql.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;

public abstract class JdbcTable
        implements SystemTable
{
    @Override
    public final Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    /**
     * @param constraint a {@link Constraint} using {@link io.prestosql.connector.system.SystemColumnHandle} to identify columns
     */
    /*
     * This method is not part of the SystemTable interface, because system tables do not operate on column handles,
     * and without column handles it's currently not possible to express Constraint or ConstraintApplicationResult.
     * TODO provide equivalent API in the SystemTable interface
     */
    public TupleDomain<ColumnHandle> applyFilter(ConnectorSession session, Constraint constraint)
    {
        return constraint.getSummary();
    }
}
