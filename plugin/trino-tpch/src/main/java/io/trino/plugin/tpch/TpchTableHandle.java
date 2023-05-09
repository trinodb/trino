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
package io.trino.plugin.tpch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TpchTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final double scaleFactor;
    private final TupleDomain<ColumnHandle> constraint;

    public TpchTableHandle(String schemaName, String tableName, double scaleFactor)
    {
        this(schemaName, tableName, scaleFactor, TupleDomain.all());
    }

    @JsonCreator
    public TpchTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("scaleFactor") double scaleFactor,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        checkArgument(scaleFactor > 0, "Scale factor must be larger than 0");
        this.scaleFactor = scaleFactor;
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public double getScaleFactor()
    {
        return scaleFactor;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @Override
    public String toString()
    {
        return schemaName + ":" + tableName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, scaleFactor, constraint);
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
        TpchTableHandle other = (TpchTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.scaleFactor, other.scaleFactor) &&
                Objects.equals(this.constraint, other.constraint);
    }

    @Override
    public boolean equalsForFusion(Object o)
    {
        return equals(o);
    }

    @Override
    public int hashCodeForFusion()
    {
        return hashCode();
    }
}
