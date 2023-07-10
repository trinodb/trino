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
package io.trino.plugin.hive.aws.athena.projection;

import com.google.common.collect.ImmutableList;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.metastore.MetastoreUtil.canConvertSqlTypeToStringForParts;
import static io.trino.plugin.hive.metastore.MetastoreUtil.sqlScalarToString;

public class InjectedProjection
        extends Projection
{
    public InjectedProjection(String columnName)
    {
        super(columnName);
    }

    @Override
    public List<String> getProjectedValues(Optional<Domain> partitionValueFilter)
    {
        Domain domain = partitionValueFilter
                .orElseThrow(() -> invalidProjectionException(getColumnName(), "Injected projection requires single predicate for it's column in where clause"));
        Type type = domain.getType();
        if (!domain.isNullableSingleValue() || !canConvertSqlTypeToStringForParts(type, true)) {
            throw invalidProjectionException(getColumnName(), "Injected projection requires single predicate for it's column in where clause. Currently provided can't be converted to single partition.");
        }
        return Optional.ofNullable(sqlScalarToString(type, domain.getNullableSingleValue(), null))
                .map(ImmutableList::of)
                .orElseThrow(() -> unsupportedProjectionColumnTypeException(type));
    }
}
