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
package io.trino.spi.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public class PrimaryKey
{
    private final Optional<String> schema;
    private final String table;
    private final String column;
    private final int sequence;
    private final Optional<String> pkname;

    public PrimaryKey(String schema, String table, String column, int sequence, String pkname)
    {
        this.schema = Optional.ofNullable(schema);
        this.table = requireNonNull(table, "table is null");
        this.column = requireNonNull(column, "column is null");
        this.sequence = sequence;
        this.pkname = Optional.ofNullable(pkname);
    }

    @JsonCreator
    public PrimaryKey(
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("table") String table,
            @JsonProperty("column") String column,
            @JsonProperty("sequence") int sequence,
            @JsonProperty("pkname") Optional<String> pkname)
    {
        this.schema = schema;
        this.table = requireNonNull(table, "table is null");
        this.column = requireNonNull(column, "column is null");
        this.sequence = sequence;
        this.pkname = pkname;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema.orElse(null);
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public String getColumn()
    {
        return column;
    }

    @JsonProperty
    public int getSequence()
    {
        return sequence;
    }

    @JsonProperty
    public String getPrimaryKey()
    {
        return pkname.orElse(null);
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        schema.ifPresent(value -> joiner.add("schema=" + value));
        joiner.add("table=" + table);
        joiner.add("column=" + column);
        joiner.add("sequence=" + Integer.valueOf(sequence).toString());
        pkname.ifPresent(value -> joiner.add("pkname=" + value));
        return getClass().getSimpleName() + joiner.toString();
    }
}
