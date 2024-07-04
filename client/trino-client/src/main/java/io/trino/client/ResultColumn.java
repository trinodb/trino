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
package io.trino.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class ResultColumn
        extends Column
{
    private final String catalog;
    private final Optional<String> schema;
    private final String table;
    private final String label;
    private final boolean autoincrement;
    private final boolean currency;
    private final boolean signed;
    private final int nullable;
    private final int precision;
    private final int scale;

    @JsonCreator
    public ResultColumn(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("typeSignature") ClientTypeSignature typeSignature,
            @JsonProperty("catalog") String catalog,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("table") String table,
            @JsonProperty("label") String label,
            @JsonProperty("autoincrement") boolean autoincrement,
            @JsonProperty("currency") boolean currency,
            @JsonProperty("signed") boolean signed,
            @JsonProperty("nullable") int nullable,
            @JsonProperty("precision") int precision,
            @JsonProperty("scale") int scale)
    {
        super(name, type, typeSignature);
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.label = requireNonNull(label, "label is null");
        this.autoincrement = autoincrement;
        this.currency = currency;
        this.signed = signed;
        this.nullable = nullable;
        this.precision = precision;
        this.scale = scale;
    }

    @JsonProperty
    public String getCatalog()
    {
        return catalog;
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
    public String getLabel()
    {
        return label;
    }

    @JsonProperty
    public boolean isAutoIncrement()
    {
        return autoincrement;
    }

    @JsonProperty
    public boolean isCurrency()
    {
        return currency;
    }

    @JsonProperty
    public boolean isSigned()
    {
        return signed;
    }

    @JsonProperty
    public int getNullable()
    {
        return nullable;
    }

    @JsonProperty
    public int getPrecision()
    {
        return precision;
    }

    @JsonProperty
    public int getScale()
    {
        return scale;
    }
}
