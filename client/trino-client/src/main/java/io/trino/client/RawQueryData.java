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

import java.util.List;

import static com.google.common.collect.Iterables.unmodifiableIterable;
import static io.trino.client.FixJsonDataUtils.fixData;

/**
 * Class represents QueryData serialized to JSON array of arrays of objects.
 * It has custom handling and representation in the {@link QueryDataClientJacksonModule}
 */
public class RawQueryData
        implements QueryData
{
    private final Iterable<List<Object>> iterable;

    private RawQueryData(Iterable<List<Object>> values)
    {
        this.iterable = values == null ? null : unmodifiableIterable(values);
    }

    @Override
    public Iterable<List<Object>> getData()
    {
        return iterable;
    }

    public static QueryData of(@Nullable Iterable<List<Object>> values)
    {
        return new RawQueryData(values);
    }

    // JSON encoding loses type information. In order for it to be usable, we need to fix types.
    public QueryData fixTypes(List<Column> columns)
    {
        return RawQueryData.of(fixData(columns, iterable));
    }
}
