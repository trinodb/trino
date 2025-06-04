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
package io.trino.plugin.hive;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;

import java.util.Optional;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_PARTITION_NOT_READABLE;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_TABLE_READ_ONLY;
import static java.lang.String.format;

public class HiveNotReadableException
        extends TrinoException
{
    public HiveNotReadableException(SchemaTableName tableName, Optional<String> partition, String message)
    {
        super(partition.isPresent() ? HIVE_PARTITION_NOT_READABLE : HIVE_TABLE_READ_ONLY, composeMessage(tableName, partition, message));
    }

    private static String composeMessage(SchemaTableName tableName, Optional<String> partition, String message)
    {
        return partition.isPresent()
                ? format("Table '%s' partition '%s' is not readable: %s", tableName, partition.get(), message)
                : format("Table '%s' is not readable: %s", tableName, message);
    }
}
