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
package io.trino.spi.metastore.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.metastore.util.MetastoreUtil;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * column entity
 *
 * @since 2020-03-16
 */
public class ColumnEntity
{
    private String name;
    private String type;
    private String comment;
    private Map<String, String> parameters = new LinkedHashMap<>(MetastoreUtil.PARAMETER_NUMBER);

    /**
     * construction
     */
    public ColumnEntity()
    {
    }

    /**
     * construction
     *
     * @param name column name
     * @param type column type
     * @param comment comment
     * @param parameters parameters of column
     */
    @JsonCreator
    public ColumnEntity(@JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("comment") String comment,
            @JsonProperty("parameters") Map<String, String> parameters)
    {
        this.name = requireNonNull(name, "column name is null");
        this.type = requireNonNull(type, "column type is null");
        this.comment = comment;
        this.parameters = parameters;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    @JsonProperty
    public String getComment()
    {
        return comment;
    }

    public void setComment(String comment)
    {
        this.comment = comment;
    }

    @JsonProperty
    public Map<String, String> getParameters()
    {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters)
    {
        this.parameters = parameters;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnEntity that = (ColumnEntity) o;
        return Objects.equals(name, that.name)
                && Objects.equals(type, that.type)
                && Objects.equals(comment, that.comment)
                && Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, comment, parameters);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", name)
                .add("type", type)
                .add("comment", comment)
                .add("parameters", parameters)
                .toString();
    }
}
