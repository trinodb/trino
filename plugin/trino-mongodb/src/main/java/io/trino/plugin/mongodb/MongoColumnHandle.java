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
        dereferenceNames = ImmutableList.copyOf(requireNonNull(dereferenceNames, "dereferenceNames is null"));
        requireNonNull(type, "type is null");
        requireNonNull(comment, "comment is null");
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
