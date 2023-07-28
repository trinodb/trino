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

import jakarta.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.unmodifiableIterable;
import static io.trino.client.QueryDataEncodings.singleEncoding;
import static io.trino.client.QueryDataReference.INLINE;
import static java.util.Objects.requireNonNull;

public class InlineQueryDataDecoder
        implements QueryDataDecoder
{
    private final QueryDataDeserializer deserializer;

    public InlineQueryDataDecoder(QueryDataDeserializer deserializer)
    {
        this.deserializer = requireNonNull(deserializer, "deserializer is null");
    }

    @Override
    public Iterable<List<Object>> decode(@Nullable QueryData data, List<Column> columns)
    {
        if (data == null) {
            return null;
        }

        verify(data instanceof EncodedQueryData, "Data is not encoded");
        EncodedQueryData encodedQueryData = (EncodedQueryData) data;

        return unmodifiableIterable(concat(encodedQueryData.getParts().stream()
                .map(part -> decode(part, columns))
                .collect(toImmutableList())));
    }

    public Iterable<List<Object>> decode(QueryDataPart part, List<Column> columns)
    {
        return deserializer.deserialize(new ByteArrayInputStream(part.getValue()), columns);
    }

    @Override
    public QueryDataEncodings consumes()
    {
        return singleEncoding(INLINE, deserializer.getSerialization());
    }
}
