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
package io.trino.plugin.hive.aws.athena;

import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;

import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static java.lang.String.format;

public class InvalidProjectionException
        extends TrinoException
{
    public InvalidProjectionException(String columnName, Type columnType)
    {
        this(columnName, "Unsupported column type: " + columnType.getDisplayName());
    }

    public InvalidProjectionException(String columnName, String message)
    {
        this(invalidProjectionMessage(columnName, message));
    }

    public InvalidProjectionException(String message)
    {
        super(INVALID_COLUMN_PROPERTY, message);
    }

    public static String invalidProjectionMessage(String columnName, String message)
    {
        return format("Column projection for column '%s' failed. %s", columnName, message);
    }
}
