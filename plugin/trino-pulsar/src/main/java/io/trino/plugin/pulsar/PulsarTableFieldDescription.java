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
package io.trino.plugin.pulsar;

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Json description to parse a single field from a Redis key/value row. See {@link RedisTableDescription} for more details.
 */
public record PulsarTableFieldDescription(
        String name,
        Type type,
        String mapping,
        String comment,
        String dataFormat,
        String formatHint,
        boolean hidden)
{
    public PulsarTableFieldDescription
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        requireNonNull(type, "type is null");
    }

    PulsarColumnHandle columnHandle(boolean keyDecoder, int index)
    {
        return new PulsarColumnHandle(Integer.toString(index), name(), type(), mapping(), dataFormat(), formatHint(), keyDecoder, hidden(), false, null);
    }

    ColumnMetadata columnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(name())
                .setType(type())
                .setComment(Optional.ofNullable(comment()))
                .setHidden(hidden())
                .build();
    }
}
