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
package io.prestosql.plugin.kudu;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class KuduTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final KuduTableHandle tableHandle;
    private final TupleDomain<ColumnHandle> constraintSummary;

    @JsonCreator
    public KuduTableLayoutHandle(@JsonProperty("tableHandle") KuduTableHandle tableHandle,
            @JsonProperty("constraintSummary") TupleDomain<ColumnHandle> constraintSummary)
    {
        this.tableHandle = requireNonNull(tableHandle, "table is null");
        this.constraintSummary = constraintSummary;
    }

    @JsonProperty
    public KuduTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraintSummary()
    {
        return constraintSummary;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        KuduTableLayoutHandle other = (KuduTableLayoutHandle) obj;
        return Objects.equals(tableHandle, other.tableHandle)
                && Objects.equals(constraintSummary, other.constraintSummary);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableHandle,
                constraintSummary);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableHandle", tableHandle)
                .add("constraintSummary", constraintSummary)
                .toString();
    }
}
