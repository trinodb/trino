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
package io.trino.plugin.iceberg;

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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SystemTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class IcebergSystemTableHandle
        implements SystemTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<ColumnHandle> constraint;

    private final Optional<Long> snapshotId;

    @JsonCreator
    public IcebergSystemTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("snapshotId") Optional<Long> snapshotId,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
    {
        this.schemaName = checkSchemaName(schemaName);
        this.tableName = checkTableName(tableName);
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @JsonProperty
    @Override
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    @Override
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    @Override
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Optional<Long> getSnapshotId()
    {
        return snapshotId;
    }

    @Override
    public SystemTableHandle witConstraint(TupleDomain<ColumnHandle> constraint)
    {
        return new IcebergSystemTableHandle(schemaName, tableName, snapshotId, constraint);
    }

    @Override
    public String toString()
    {
        return schemaName + "." + tableName + "@" + snapshotId.map(v -> "@" + v).orElse("");
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, snapshotId, constraint);
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
        IcebergSystemTableHandle other = (IcebergSystemTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.snapshotId, other.snapshotId) &&
                Objects.equals(this.constraint, other.constraint);
    }

    private static String checkSchemaName(String schemaName)
    {
        return checkLowerCase(schemaName, "schemaName");
    }

    private static String checkTableName(String tableName)
    {
        return checkLowerCase(tableName, "tableName");
    }

    public static String checkLowerCase(String value, String name)
    {
        if (value == null) {
            throw new NullPointerException(format("%s is null", name));
        }
        checkArgument(value.equals(value.toLowerCase(ENGLISH)), "%s is not lowercase: %s", name, value);
        return value;
    }
}
