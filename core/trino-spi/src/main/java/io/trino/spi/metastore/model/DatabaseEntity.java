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
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * database entity
 *
 * @since 2020-03-13
 */
public class DatabaseEntity
{
    private String catalogName;
    private String name;
    private String owner;
    private long createTime;
    private String comment;
    private Map<String, String> parameters = new LinkedHashMap<>(MetastoreUtil.PARAMETER_NUMBER);

    /**
     * construction
     */
    public DatabaseEntity()
    {
    }

    /**
     * construction
     *
     * @param catalogName catalogName
     * @param name        name
     * @param owner       owner
     * @param createTime  create time
     * @param comment     comment
     * @param parameters  parameters of database
     */
    @JsonCreator
    public DatabaseEntity(@JsonProperty("catalogName") String catalogName,
            @JsonProperty("name") String name,
            @JsonProperty("owner") String owner,
            @JsonProperty("createTime") long createTime,
            @JsonProperty("comment") String comment,
            @JsonProperty("parameters") Map<String, String> parameters)
    {
        this.catalogName = requireNonNull(catalogName, "catalog name is null");
        this.name = requireNonNull(name, "database name is null");
        this.owner = owner;
        this.createTime = createTime;
        this.comment = comment;
        this.parameters = parameters;
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatabaseEntity that = (DatabaseEntity) o;
        return this.createTime == that.createTime
                && Objects.equals(catalogName, that.catalogName)
                && Objects.equals(name, that.name)
                && Objects.equals(owner, that.owner)
                && Objects.equals(comment, that.comment)
                && Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, name, owner, createTime, comment, parameters);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", name)
                .add("databaseName", name)
                .add("owner", owner)
                .add("createTime", createTime)
                .add("comment", comment)
                .add("parameters", parameters)
                .toString();
    }

    /**
     * database entity builder
     *
     * @return database entity builder
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * database entity builder
     *
     * @param database database
     * @return database entity builder
     */
    public static Builder builder(DatabaseEntity database)
    {
        return new Builder(database);
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
        private String owner;
        private long createTime;
        private Optional<String> comment = Optional.empty();
        private Map<String, String> parameters = new LinkedHashMap<>(MetastoreUtil.PARAMETER_NUMBER);

        /**
         * construction
         */
        public Builder()
        {
        }

        /**
         * construction
         *
         * @param database database
         */
        public Builder(DatabaseEntity database)
        {
            this.catalogName = database.catalogName;
            this.databaseName = database.name;
            this.owner = database.owner;
            this.createTime = database.createTime;
            this.comment = Optional.of(database.comment);
            this.parameters = database.parameters;
        }

        /**
         * set catalog name
         *
         * @param catalogName catalog name
         * @return builder
         */
        public Builder setCatalogName(String catalogName)
        {
            requireNonNull(catalogName, "catalogName is null");
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
            requireNonNull(databaseName, "databaseName is null");
            this.databaseName = databaseName;
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
            requireNonNull(owner, "owner is null");
            this.owner = owner;
            return this;
        }

        /**
         * set create time
         *
         * @param createTime createTime
         * @return builder
         */
        public Builder setCreateTime(long createTime)
        {
            this.createTime = createTime;
            return this;
        }

        /**
         * set comment
         *
         * @param comment comment
         * @return builder
         */
        public Builder setComment(Optional<String> comment)
        {
            requireNonNull(comment, "comment is null");
            this.comment = comment;
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
            requireNonNull(parameters, "parameters is null");
            this.parameters = parameters;
            return this;
        }

        /**
         * build database entity
         *
         * @return database entity
         */
        public DatabaseEntity build()
        {
            return new DatabaseEntity(
                    catalogName,
                    databaseName,
                    owner,
                    createTime,
                    comment.orElse(null),
                    parameters);
        }
    }
}
