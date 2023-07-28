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

import java.util.List;

import static com.google.common.collect.Iterables.unmodifiableIterable;
import static io.trino.client.NoQueryData.NO_DATA;
import static java.util.Objects.requireNonNull;

/**
 * Class represents QueryData serialized to JSON array of arrays of objects.
 * It has custom handling and representation in the QueryData (de)serializer.
 */
@QueryDataFormat(formatName = "json-inlined")
public class JsonInlineQueryData
        implements QueryData
{
    private final Iterable<List<Object>> values;
    private final boolean hasValues;

    JsonInlineQueryData(Iterable<List<Object>> values, boolean hasValues)
    {
        this.values = unmodifiableIterable(requireNonNull(values, "values is null"));
        this.hasValues = hasValues;
    }

    @Override
    public Iterable<List<Object>> getData()
    {
        return values;
    }

    @Override
    public boolean isPresent()
    {
        return hasValues;
    }

    public static QueryData create(Iterable<List<Object>> values, boolean hasValues)
    {
        if (values == null) {
            return NO_DATA;
        }

        return new JsonInlineQueryData(values, hasValues);
    }
}
