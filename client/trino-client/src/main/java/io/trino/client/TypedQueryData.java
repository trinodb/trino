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

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Class represents QueryData of already typed values
 *
 */
public class TypedQueryData
        implements QueryData
{
    private final List<List<Object>> values;
    private final long rowsCount;

    private TypedQueryData(List<List<Object>> values, long rowsCount)
    {
        this.values = ImmutableList.copyOf(values);
        this.rowsCount = rowsCount;
    }

    public Iterable<List<Object>> getIterable()
    {
        return values;
    }

    public static QueryData of(List<List<Object>> values)
    {
        return new TypedQueryData(values, values != null ? values.size() : 0);
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public long getRowsCount()
    {
        return rowsCount;
    }
}
