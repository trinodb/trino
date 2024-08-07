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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;
import org.bson.Document;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.mongodb.MongoSession.COLLECTION_NAME;
import static io.trino.plugin.mongodb.MongoSession.COLLECTION_NAME_NATIVE;
import static io.trino.plugin.mongodb.MongoSession.DATABASE_NAME;
import static io.trino.plugin.mongodb.MongoSession.DATABASE_NAME_NATIVE;
import static io.trino.plugin.mongodb.MongoSession.ID;
import static io.trino.plugin.mongodb.MongoSession.ID_NATIVE;
import static java.util.Objects.requireNonNull;

/**
 * @param dbRefField Represent if the field is inside a DBRef type. The getter may return a wrong value when row type use the same field names and types as dbref.
 */
public record MongoColumnHandle(String baseName, List<String> dereferenceNames, Type type, boolean hidden, boolean dbRefField, Optional<String> comment)
        implements ColumnHandle
{
    public MongoColumnHandle
    {
        requireNonNull(baseName, "baseName is null");
        requireNonNull(dereferenceNames, "dereferenceNames is null");
        requireNonNull(type, "type is null");
        requireNonNull(comment, "comment is null");

        if (dbRefField) {
            String leafColumnName = dereferenceNames.getLast();
            String leafDBRefNativeName = switch (leafColumnName) {
                case DATABASE_NAME -> DATABASE_NAME_NATIVE;
                case COLLECTION_NAME -> COLLECTION_NAME_NATIVE;
                case ID -> ID_NATIVE;
                default -> leafColumnName;
            };
            dereferenceNames = ImmutableList.<String>builder()
                .addAll(dereferenceNames.subList(0, dereferenceNames.size() - 1))
                .add(leafDBRefNativeName)
                .build();
        }
        else {
            dereferenceNames = ImmutableList.copyOf(dereferenceNames);
        }
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
    public String toString()
    {
        return getQualifiedName() + ":" + type;
    }
}
