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
package io.trino.plugin.accumulo.model;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record AccumuloColumnHandle(
        String name,
        Optional<String> family,
        Optional<String> qualifier,
        Type type,
        int ordinal,
        String extraInfo,
        Optional<String> comment,
        boolean indexed)
        implements ColumnHandle, Comparable<AccumuloColumnHandle>
{
    public AccumuloColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(family, "family is null");
        requireNonNull(qualifier, "qualifier is null");
        requireNonNull(type, "type is null");
        checkArgument(ordinal >= 0, "ordinal must be >= zero");
        requireNonNull(extraInfo, "extraInfo is null");
        requireNonNull(comment, "comment is null");
    }

    public ColumnMetadata columnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setExtraInfo(Optional.ofNullable(extraInfo))
                .setComment(comment)
                .build();
    }

    @Override
    public int compareTo(AccumuloColumnHandle obj)
    {
        return Integer.compare(this.ordinal(), obj.ordinal());
    }
}
