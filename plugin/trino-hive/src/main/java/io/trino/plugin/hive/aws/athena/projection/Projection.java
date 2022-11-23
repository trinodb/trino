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

import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class Projection
{
    private final String columnName;

    public Projection(String columnName)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
    }

    public String getColumnName()
    {
        return columnName;
    }

    public abstract List<String> getProjectedValues(Optional<Domain> partitionValueFilter);

    protected TrinoException unsupportedProjectionColumnTypeException(Type columnType)
    {
        return unsupportedProjectionColumnTypeException(columnName, columnType);
    }

    public static TrinoException unsupportedProjectionColumnTypeException(String columnName, Type columnType)
    {
        return invalidProjectionException(columnName, "Unsupported column type: " + columnType.getDisplayName());
    }

    public static TrinoException invalidProjectionException(String columnName, String message)
    {
        throw new TrinoException(INVALID_COLUMN_PROPERTY, invalidProjectionMessage(columnName, message));
    }

    public static String invalidProjectionMessage(String columnName, String message)
    {
        return format("Column projection for column '%s' failed. %s", columnName, message);
    }
}
