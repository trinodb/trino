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
package io.prestosql.metadata;

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.expression.ConnectorExpression;

import java.util.List;

public class FilterApplicationResult
{
    private final TableHandle table;
    private final ConnectorExpression remainingFilter;
    private final List<Column> newProjections;

    public FilterApplicationResult(TableHandle table, ConnectorExpression remainingFilter, List<Column> newProjections)
    {
        this.table = table;
        this.remainingFilter = remainingFilter;
        this.newProjections = newProjections;
    }

    public TableHandle getTable()
    {
        return table;
    }

    public ConnectorExpression getRemainingFilter()
    {
        return remainingFilter;
    }

    public List<Column> getNewProjections()
    {
        return newProjections;
    }

    public static class Column
    {
        private final ColumnHandle column;
        private final Type type;

        public Column(ColumnHandle column, Type type)
        {
            this.column = column;
            this.type = type;
        }

        public ColumnHandle getColumn()
        {
            return column;
        }

        public Type getType()
        {
            return type;
        }
    }
}
