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
package io.trino.plugin.prometheus;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.prometheus.expression.LabelFilterExpression;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public record PrometheusTableHandle(String schemaName, String tableName, TupleDomain<ColumnHandle> predicate, List<LabelFilterExpression> expressions)
        implements ConnectorTableHandle
{
    public PrometheusTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(predicate, "predicate is null");

        // Throws NullPointerException if collection contains null element
        expressions = ImmutableList.copyOf(expressions);
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    public PrometheusTableHandle withPredicate(TupleDomain<ColumnHandle> predicate)
    {
        return new PrometheusTableHandle(schemaName, tableName, predicate, expressions);
    }

    public PrometheusTableHandle withExpressions(List<LabelFilterExpression> expressions)
    {
        return new PrometheusTableHandle(schemaName, tableName, predicate, expressions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, expressions);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        PrometheusTableHandle other = (PrometheusTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                this.expressions.size() == other.expressions.size() &&
                new HashSet<>(this.expressions).containsAll(other.expressions);
    }

    @Override
    public String toString()
    {
        return schemaName + ":" + tableName;
    }
}
