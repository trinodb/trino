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
package io.trino.plugin.mongodb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;
import org.bson.Document;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class MongoColumnHandle
        implements ColumnHandle
{
    private final String baseName;
    private final List<String> dereferenceNames;
    private final Type type;
    private final boolean hidden;
    // Represent if the field is inside a DBRef type
    private final boolean dbRefField;
    private final Optional<String> comment;

    @JsonCreator
    public MongoColumnHandle(
            @JsonProperty("baseName") String baseName,
            @JsonProperty("dereferenceNames") List<String> dereferenceNames,
            @JsonProperty("columnType") Type type,
            @JsonProperty("hidden") boolean hidden,
            @JsonProperty("dbRefField") boolean dbRefField,
            @JsonProperty("comment") Optional<String> comment)
    {
        this.baseName = requireNonNull(baseName, "baseName is null");
        this.dereferenceNames = ImmutableList.copyOf(requireNonNull(dereferenceNames, "dereferenceNames is null"));
        this.type = requireNonNull(type, "type is null");
        this.hidden = hidden;
        this.dbRefField = dbRefField;
        this.comment = requireNonNull(comment, "comment is null");
    }

    @JsonProperty
    public String getBaseName()
    {
        return baseName;
    }

    @JsonProperty
    public List<String> getDereferenceNames()
    {
        return dereferenceNames;
    }

    @JsonProperty("columnType")
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    /**
     * This method may return a wrong value when row type use the same field names and types as dbref.
     */
    @JsonProperty
    public boolean isDbRefField()
    {
        return dbRefField;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    public ColumnMetadata toColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(getQualifiedName())
                .setType(type)
                .setHidden(hidden)
                .setComment(comment)
                .build();
    }

    @JsonIgnore
    public String getQualifiedName()
    {
        return Joiner.on('.')
                .join(ImmutableList.<String>builder()
                        .add(baseName)
                        .addAll(dereferenceNames)
                        .build());
    }

    @JsonIgnore
    public boolean isBaseColumn()
    {
        return dereferenceNames.isEmpty();
    }

    public Document getDocument()
    {
        return new Document().append("name", getQualifiedName())
                .append("type", type.getTypeSignature().toString())
                .append("hidden", hidden)
                .append("dbRefField", dbRefField)
                .append("comment", comment.orElse(null));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(baseName, dereferenceNames, type, hidden, dbRefField, comment);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MongoColumnHandle other = (MongoColumnHandle) obj;
        return Objects.equals(baseName, other.baseName) &&
                Objects.equals(dereferenceNames, other.dereferenceNames) &&
                Objects.equals(type, other.type) &&
                Objects.equals(hidden, other.hidden) &&
                Objects.equals(dbRefField, other.dbRefField) &&
                Objects.equals(comment, other.comment);
    }

    @Override
    public String toString()
    {
        return getQualifiedName() + ":" + type;
    }
}
