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
package io.prestosql.plugin.phoenix;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PhoenixOutputTableHandle
        extends JdbcOutputTableHandle
{
    private final Optional<String> rowkeyColumn;

    @JsonCreator
    public PhoenixOutputTableHandle(
            @JsonProperty("schemaName") Optional<String> schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("jdbcColumnTypes") Optional<List<JdbcTypeHandle>> jdbcColumnTypes,
            @JsonProperty("rowkeyColumn") Optional<String> rowkeyColumn)
    {
        super("", schemaName.orElse(null), tableName, columnNames, columnTypes, jdbcColumnTypes, "");
        this.rowkeyColumn = requireNonNull(rowkeyColumn, "rowkeyColumn is null");
    }

    @JsonProperty
    public Optional<String> rowkeyColumn()
    {
        return rowkeyColumn;
    }
}
