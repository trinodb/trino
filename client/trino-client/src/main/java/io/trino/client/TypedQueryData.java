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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.unmodifiableIterable;

/**
 * Class represents QueryData of already typed values
 *
 */
public class TypedQueryData
        implements QueryData
{
    private final Iterable<List<Object>> iterable;

    private TypedQueryData(Iterable<List<Object>> values)
    {
        this.iterable = values == null ? null : unmodifiableIterable(values);
    }

    public Iterable<List<Object>> getIterable()
    {
        checkState(iterable != null, "cannot return a null iterable");
        return iterable;
    }

    public static QueryData of(@Nullable Iterable<List<Object>> values)
    {
        return new TypedQueryData(values);
    }

    @Override
    public boolean isNull()
    {
        return iterable == null;
    }
}
