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
package io.trino.plugin.deltalake.metastore;

import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NotADeltaLakeTableException
        extends TrinoException
{
    public NotADeltaLakeTableException(String databaseName, String tableName)
    {
        super(
                UNSUPPORTED_TABLE_TYPE,
                format("%s.%s is not a Delta Lake table", requireNonNull(databaseName, "databaseName is null"), requireNonNull(tableName, "tableName is null")));
    }
}
