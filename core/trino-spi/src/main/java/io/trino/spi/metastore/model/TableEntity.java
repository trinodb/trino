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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.spi.metastore.util.MetastoreUtil.PARAMETER_NUMBER;
import static java.util.Objects.requireNonNull;

/**
 * table entity
 *
 * @since 2020-03-13
 */
public class TableEntity
{
    private String catalogName;
    private String databaseName;
    private String name;
    private String owner;
    private String type;
    private String viewOriginalText;
    private long createTime;
    private String comment;
    private Map<String, String> parameters = new LinkedHashMap<>(PARAMETER_NUMBER);
    private List<ColumnEntity> columns = new ArrayList<>(PARAMETER_NUMBER);

    /**
     * construction
     */
    public TableEntity()
    {
    }

    /**
     * construction
     *
     * @param catalogName catalog name
     * @param databaseName database name
     * @param name table name
     * @param owner owner
     * @param type table type
     * @param viewOriginalText view sql
     * @param createTime create time
     * @param comment comment
     * @param parameters paramters
     * @param columns columns
     */
    @JsonCreator
    public TableEntity(@JsonProperty("catalogName") String catalogName,
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("name") String name,
            @JsonProperty("owner") String owner,
            @JsonProperty("type") String type,
            @JsonProperty("viewOriginalText") String viewOriginalText,
            @JsonProperty("createTime") long createTime,
            @JsonProperty("comment") String comment,
            @JsonProperty("parameters") Map<String, String> parameters,
            @JsonProperty("columns") List<ColumnEntity> columns)
    {
        this.catalogName = requireNonNull(catalogName, "catalog name is null");
        this.databaseName = requireNonNull(databaseName, "database name is null");
        this.name = requireNonNull(name, "table name is null");
        this.type = requireNonNull(type, "table type is null");
        this.viewOriginalText = viewOriginalText;
        this.owner = owner;
        this.createTime = createTime;
        this.comment = comment;
        this.parameters = parameters;
        this.columns = columns;
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalogName;
    }

    public void setCatalogName(String catalogName)
    {
        this.catalogName = catalogName;
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
    public String getOwner()
    {
        return owner;
    }

    public void setOwner(String owner)
    {
        this.owner = owner;
    }

    @JsonProperty
    public long getCreateTime()
    {
        return createTime;
    }

    public void setCreateTime(long createTime)
    {
        this.createTime = createTime;
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
    public String getDatabaseName()
    {
        return databaseName;
    }

    public void setDatabaseName(String databaseName)
    {
        this.databaseName = databaseName;
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

    @JsonProperty
    public String getViewOriginalText()
    {
        return viewOriginalText;
    }

    public void setViewOriginalText(String viewOriginalText)
    {
        this.viewOriginalText = viewOriginalText;
    }

    @JsonProperty
    public List<ColumnEntity> getColumns()
    {
        return columns;
    }

    public void setColumns(List<ColumnEntity> columns)
    {
        this.columns = columns;
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableEntity that = (TableEntity) o;
        return this.createTime == that.createTime
                && Objects.equals(catalogName, that.catalogName)
                && Objects.equals(databaseName, that.databaseName)
                && Objects.equals(name, that.name)
                && Objects.equals(owner, that.owner)
                && Objects.equals(type, that.type)
                && Objects.equals(viewOriginalText, that.viewOriginalText)
                && Objects.equals(comment, that.comment)
                && Objects.equals(parameters, that.parameters)
                && Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, databaseName, name, owner, type, viewOriginalText,
                createTime, comment, parameters, columns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", catalogName)
                .add("databaseName", databaseName)
                .add("name", name)
                .add("type", owner)
                .add("viewOriginalText", viewOriginalText)
                .add("owner", owner)
                .add("createTime", createTime)
                .add("comment", comment)
                .add("parameters", parameters)
                .add("columns", columns)
                .toString();
    }

    /**
     * table builder
     *
     * @return builder
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * table builder
     *
     * @param table table
     * @return builder
     */
    public static Builder builder(TableEntity table)
    {
        return new Builder(table);
    }

    /**
     * builder
     *
     * @since 2020-03-16
     */
    public static class Builder
    {
        private String catalogName;
        private String databaseName;
        private String tableName;
        private String tableType;
        private Optional<String> viewOriginalText = Optional.empty();
        private long createTime;
        private String owner;
        private String comment;
        private List<ColumnEntity> columns = new ArrayList<>(PARAMETER_NUMBER);
        private Map<String, String> parameters = new LinkedHashMap<>(PARAMETER_NUMBER);

        private Builder()
        {
        }

        private Builder(TableEntity table)
        {
            catalogName = table.catalogName;
            databaseName = table.databaseName;
            tableName = table.name;
            tableType = table.type;
            viewOriginalText = table.viewOriginalText != null ? Optional.of(table.viewOriginalText) : Optional.empty();
            createTime = table.createTime;
            owner = table.owner;
            comment = table.comment;
            columns = new ArrayList<>(table.columns);
            parameters = new LinkedHashMap<>(table.parameters);
        }

        /**
         * set catalog anme
         *
         * @param catalogName catalog name
         * @return builder
         */
        public Builder setCatalogName(String catalogName)
        {
            this.catalogName = catalogName;
            return this;
        }

        /**
         * set database name
         *
         * @param databaseName database name
         * @return builder
         */
        public Builder setDatabaseName(String databaseName)
        {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * set table name
         *
         * @param tableName table name
         * @return builder
         */
        public Builder setTableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        /**
         * set table type
         *
         * @param tableType table type
         * @return builder
         */
        public Builder setTableType(String tableType)
        {
            this.tableType = tableType;
            return this;
        }

        /**
         * set viewOriginalText
         *
         * @param viewOriginalText viewOriginalText
         * @return builder
         */
        public Builder setViewOriginalText(Optional<String> viewOriginalText)
        {
            this.viewOriginalText = viewOriginalText;
            return this;
        }

        /**
         * set create time
         *
         * @param createTime createTime
         * @return builer
         */
        public Builder setCreateTime(long createTime)
        {
            this.createTime = createTime;
            return this;
        }

        /**
         * set owner
         *
         * @param owner owner
         * @return builder
         */
        public Builder setOwner(String owner)
        {
            this.owner = owner;
            return this;
        }

        /**
         * set comment
         *
         * @param comment comment
         * @return builder
         */
        public Builder setComment(String comment)
        {
            this.comment = comment;
            return this;
        }

        /**
         * set columns
         *
         * @param columns columns
         * @return builder
         */
        public Builder setColumns(List<ColumnEntity> columns)
        {
            this.columns = new ArrayList<>(columns);
            return this;
        }

        /**
         * add column
         *
         * @param column column
         * @return builder
         */
        public Builder addColumn(ColumnEntity column)
        {
            this.columns.add(column);
            return this;
        }

        /**
         * set parameters
         *
         * @param parameters parameters
         * @return builder
         */
        public Builder setParameters(Map<String, String> parameters)
        {
            this.parameters = new LinkedHashMap<>(parameters);
            return this;
        }

        /**
         * set parameter
         *
         * @param key key
         * @param value value
         * @return builder
         */
        public Builder setParameter(String key, String value)
        {
            this.parameters.put(key, value);
            return this;
        }

        /**
         * build table
         *
         * @return table
         */
        public TableEntity build()
        {
            return new TableEntity(
                    catalogName,
                    databaseName,
                    tableName,
                    owner,
                    tableType,
                    viewOriginalText.orElse(null),
                    createTime,
                    comment,
                    parameters,
                    columns);
        }
    }
}
