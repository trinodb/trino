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
package io.trino.plugin.hive.projection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.metastore.MetastoreUtil.canConvertSqlTypeToStringForParts;
import static io.trino.plugin.hive.metastore.MetastoreUtil.sqlScalarToString;
import static java.util.Objects.requireNonNull;

public final class InjectedProjection
        implements Projection
{
    private final String columnName;

    @JsonCreator
    public InjectedProjection(@JsonProperty("columnName") String columnName)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
    }

    public InjectedProjection(String columnName, Type columnType)
    {
        if (!canConvertSqlTypeToStringForParts(columnType, true)) {
            throw new InvalidProjectionException(columnName, columnType);
        }
        this.columnName = requireNonNull(columnName, "columnName is null");
    }

    @Override
    public List<String> getProjectedValues(Optional<Domain> partitionValueFilter)
    {
        Domain domain = partitionValueFilter
                .orElseThrow(() -> new InvalidProjectionException(columnName, "Injected projection requires single predicate for it's column in where clause"));
        Type type = domain.getType();
        ValueSet values = domain.getValues();
        if (!values.isDiscreteSet() || !canConvertSqlTypeToStringForParts(type, true)) {
            throw new InvalidProjectionException(columnName, "Injected projection requires single predicate for it's column in where clause. Currently provided can't be converted to single partition.");
        }
        return values.getDiscreteSet().stream()
                .map(value -> {
                    String stringValue = sqlScalarToString(type, value, null);
                    if (stringValue == null) {
                        throw new InvalidProjectionException(columnName, type);
                    }
                    return stringValue;
                })
                .collect(toImmutableList());
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }
}
