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
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/// Tree-form [Json] object. Members are stored in strict insertion order; duplicate keys
/// (allowed under SQL:2023 §9.42 `WITHOUT UNIQUE KEYS`) appear as separate
/// [JsonObjectMember] entries at their original positions. A key-bytes → member-positions
/// index is built eagerly so [#objectMember] is O(1). The byte encoding is materialized
/// lazily on first [#encoding] / [#backingSlice] call and then cached.
public final class JsonObject
        implements Json
{
    private final List<JsonObjectMember> members;
    // key bytes -> member positions. For the common unique-key case the array
    // is length 1; duplicate keys grow it in insertion order.
    private final Map<Slice, int[]> index;
    private volatile Slice encoded;

    public JsonObject(List<JsonObjectMember> members)
    {
        requireNonNull(members, "members is null");
        this.members = ImmutableList.copyOf(members);
        this.index = buildIndex(this.members);
    }

    public List<JsonObjectMember> members()
    {
        return members;
    }

    public boolean hasDuplicateKeys()
    {
        return index.size() != members.size();
    }

    @Override
    public Kind kind()
    {
        return Kind.OBJECT;
    }

    @Override
    public boolean isObject()
    {
        return true;
    }

    @Override
    public int objectSize()
    {
        return members.size();
    }

    @Override
    public Optional<Json> objectMember(Slice keyBytes)
    {
        requireNonNull(keyBytes, "keyBytes is null");
        int[] positions = index.get(keyBytes);
        if (positions == null) {
            return Optional.empty();
        }
        return Optional.of(members.get(positions[0]).value());
    }

    @Override
    public void objectMembers(Slice keyBytes, Consumer<Json> consumer)
    {
        requireNonNull(keyBytes, "keyBytes is null");
        requireNonNull(consumer, "consumer is null");
        int[] positions = index.get(keyBytes);
        if (positions == null) {
            return;
        }
        for (int position : positions) {
            consumer.accept(members.get(position).value());
        }
    }

    @Override
    public void forEachObjectMember(BiConsumer<String, Json> consumer)
    {
        requireNonNull(consumer, "consumer is null");
        for (JsonObjectMember member : members) {
            consumer.accept(member.key().toStringUtf8(), member.value());
        }
    }

    @Override
    public void forEachObjectMemberBytes(BiConsumer<Slice, Json> consumer)
    {
        requireNonNull(consumer, "consumer is null");
        for (JsonObjectMember member : members) {
            consumer.accept(member.key(), member.value());
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
        // Racy single-check on a volatile field: encodeTree is deterministic, so concurrent
        // callers may each encode and the last write wins. The volatile read/write
        // guarantees safe publication; any reader sees the fully written Slice. A redundant
        // encode on the first race is bounded; subsequent calls hit the cache.
        Slice value = encoded;
        if (value == null) {
            value = JsonItems.encodeTree(this).encoding();
            encoded = value;
        }
        return value;
    }

    private static Map<Slice, int[]> buildIndex(List<JsonObjectMember> members)
    {
        // Duplicate keys are legal (WITHOUT UNIQUE KEYS), and raw connector JSON reaches this
        // on first structural access — so an object of n members sharing one key must stay
        // linear. Collect positions with amortized growth, then compact once per key.
        int size = members.size();
        // Exact-size init: sized for `size` entries so the map never rehashes during
        // access. HashMap(size) treats size as the initial bucket count, not capacity.
        Map<Slice, IntArrayList> positions = HashMap.newHashMap(size);
        for (int i = 0; i < size; i++) {
            positions.computeIfAbsent(members.get(i).key(), _ -> new IntArrayList(1)).add(i);
        }

        Map<Slice, int[]> built = HashMap.newHashMap(positions.size());
        positions.forEach((key, list) -> built.put(key, list.toIntArray()));
        return built;
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
        return "Json[OBJECT, " + members.size() + " members]";
    }
}
