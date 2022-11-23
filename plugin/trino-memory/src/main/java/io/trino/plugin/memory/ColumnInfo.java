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
package io.trino.plugin.memory;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ColumnInfo
{
    private final ColumnHandle handle;
    private final String name;
    private final Type type;

    private final Optional<String> comment;

    public ColumnInfo(ColumnHandle handle, String name, Type type, Optional<String> comment)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    public ColumnHandle getHandle()
    {
        return handle;
    }

    public String getName()
    {
        return name;
    }

    public ColumnMetadata getMetadata()
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setComment(comment)
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("comment", comment)
                .toString();
    }
}
