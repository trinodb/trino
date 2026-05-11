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
package io.trino.json;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/// Tree-form [Json] array. Elements are stored in source order; each element is itself a
/// [Json] (shared by reference, never wrapped). The byte encoding is materialized lazily on
/// first [#encoding] / [#backingSlice] call and then cached.
public final class JsonArray
        implements Json
{
    private final List<Json> elements;
    private volatile Slice encoded;

    public JsonArray(List<Json> elements)
    {
        requireNonNull(elements, "elements is null");
        this.elements = ImmutableList.copyOf(elements);
    }

    public List<Json> elements()
    {
        return elements;
    }

    @Override
    public Kind kind()
    {
        return Kind.ARRAY;
    }

    @Override
    public boolean isArray()
    {
        return true;
    }

    @Override
    public int arraySize()
    {
        return elements.size();
    }

    @Override
    public Json arrayElement(int index)
    {
        return elements.get(index);
    }

    @Override
    public void forEachArrayElement(Consumer<Json> consumer)
    {
        requireNonNull(consumer, "consumer is null");
        for (Json element : elements) {
            consumer.accept(element);
        }
    }

    @Override
    public Slice encoding()
    {
        return materialize();
    }

    @Override
    public Slice backingSlice()
    {
        return materialize();
    }

    @Override
    public int viewOffset()
    {
        return JsonItemEncoding.rootItemOffset(materialize());
    }

    @Override
    public int viewEnd()
    {
        return materialize().length();
    }

    @VisibleForTesting
    public boolean isMaterialized()
    {
        return encoded != null;
    }

    private Slice materialize()
    {
        Slice value = encoded;
        if (value == null) {
            value = JsonItems.encodeTree(this).encoding();
            encoded = value;
        }
        return value;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Json that)) {
            return false;
        }
        return JsonItemSemantics.equal(this, that);
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(JsonItemSemantics.hash(this));
    }

    @Override
    public String toString()
    {
        return "Json[ARRAY, " + elements.size() + " elements]";
    }
}
