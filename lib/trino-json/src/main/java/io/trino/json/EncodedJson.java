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

import io.airlift.slice.Slice;
import io.trino.json.JsonItemEncoding.ItemTag;
import io.trino.json.JsonItemEncoding.TypeTag;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.trino.json.JsonItemEncoding.arrayIndexedElementOffset;
import static io.trino.json.JsonItemEncoding.arrayItemsStart;
import static io.trino.json.JsonItemEncoding.copyItemEncoding;
import static io.trino.json.JsonItemEncoding.itemEndOffset;
import static io.trino.json.JsonItemEncoding.itemTag;
import static io.trino.json.JsonItemEncoding.objectEntriesStart;
import static io.trino.json.JsonItemEncoding.objectIndexedEntryEnd;
import static io.trino.json.JsonItemEncoding.objectIndexedEntryOffset;
import static io.trino.json.JsonItemEncoding.objectIndexedSorting;
import static io.trino.json.JsonItemEncoding.readString;
import static io.trino.json.JsonItemEncoding.readStringSlice;
import static io.trino.json.JsonItemEncoding.rootItemOffset;
import static io.trino.json.JsonItemEncoding.stringEndOffset;
import static java.util.Objects.requireNonNull;

/// Byte-backed [Json]. Two modes share the same class:
///
/// * **Typed-encoded** — `slice` carries the typed-item encoding; `offset..end` is a
///   view (sub-views of larger documents share the backing slice with the parent).
///   Accessors read tag bytes directly. This is the canonical "JSON in flight" form.
///
/// * **Raw text** — `slice` carries raw JSON text (no `VERSION` prefix); `offset = 0`
///   and `end = slice.length()` always. Structural accessors lazy-parse to a tree
///   (cached in [#parsed]) and delegate. Connectors that produce already-validated text
///   ([io.trino.json.Json#unchecked]) use this mode to avoid an upfront parse on values
///   that may never be structurally accessed.
///
/// Mode is fixed at construction. The leading byte of `slice` discriminates: `VERSION`
/// means typed-encoded, anything else means raw text. Sub-views created during traversal
/// are always typed (a sub-view of raw text isn't a meaningful JSON value).
public final class EncodedJson
        implements Json
{
    private final Slice slice;
    private final int offset;
    private final int end;
    private final boolean rawText;
    // Lazy-computed for rawText mode; never written in typed mode.
    private volatile Json parsed;

    public static Json of(Slice slice)
    {
        requireNonNull(slice, "slice is null");
        int rootOffset = rootItemOffset(slice);
        int rootEnd = itemEndOffset(slice, rootOffset);
        return new EncodedJson(slice, rootOffset, rootEnd, false);
    }

    /// Wraps already-validated raw JSON text. Skips the eager parse; structural
    /// accessors lazy-parse on first call. The bytes flow through a JSON-typed block
    /// write as-is via the [Json#isRawText] / [Json#rawText] shortcut.
    public static Json unchecked(Slice rawText)
    {
        requireNonNull(rawText, "rawText is null");
        return new EncodedJson(rawText, 0, rawText.length(), true);
    }

    /// Sub-view factory used by traversal accessors. Always typed-encoded — a sub-view
    /// of raw text would carry partial JSON text, which isn't meaningful.
    static EncodedJson view(Slice slice, int offset, int end)
    {
        return new EncodedJson(slice, offset, end, false);
    }

    private EncodedJson(Slice slice, int offset, int end, boolean rawText)
    {
        this.slice = requireNonNull(slice, "slice is null");
        this.offset = offset;
        this.end = end;
        this.rawText = rawText;
    }

    /// Lazy-parsed tree for raw-text mode. Package-private so [JsonItems#writeTreeJson]
    /// can recurse directly into the parsed tree rather than round-tripping through
    /// `encoding()`.
    Json parsed()
    {
        // Racy single-check on a volatile field: parseToTree is deterministic (same input
        // produces an equal-by-content tree), so concurrent callers may each compute a tree
        // and the last write wins. The volatile read/write guarantees safe publication, so
        // any reader sees a fully constructed tree. The cost of a redundant parse on the
        // first race is bounded; on every subsequent call the cache hit avoids it. This is
        // the standard idempotent-DCL idiom, not double-checked locking.
        Json value = parsed;
        if (value == null) {
            // In raw-text mode, slice is the raw text; offset/end span the full slice.
            value = JsonItems.parseToTree(slice);
            parsed = value;
        }
        return value;
    }

    @Override
    public boolean isRawText()
    {
        return rawText;
    }

    @Override
    public Slice rawText()
    {
        if (!rawText) {
            throw new IllegalStateException("Not a raw-text Json");
        }
        return slice;
    }

    @Override
    public Kind kind()
    {
        if (rawText) {
            return parsed().kind();
        }
        return switch (itemTag(slice, offset)) {
            case JSON_NULL -> Kind.NULL;
            case ARRAY, ARRAY_INDEXED -> Kind.ARRAY;
            case OBJECT, OBJECT_INDEXED -> Kind.OBJECT;
            case TYPED_VALUE -> Kind.SCALAR;
            case JSON_ERROR -> Kind.ERROR;
        };
    }

    @Override
    public boolean isNull()
    {
        if (rawText) {
            return parsed().isNull();
        }
        return itemTag(slice, offset) == ItemTag.JSON_NULL;
    }

    @Override
    public boolean isArray()
    {
        if (rawText) {
            return parsed().isArray();
        }
        ItemTag tag = itemTag(slice, offset);
        return tag == ItemTag.ARRAY || tag == ItemTag.ARRAY_INDEXED;
    }

    @Override
    public boolean isObject()
    {
        if (rawText) {
            return parsed().isObject();
        }
        ItemTag tag = itemTag(slice, offset);
        return tag == ItemTag.OBJECT || tag == ItemTag.OBJECT_INDEXED;
    }

    @Override
    public boolean isScalar()
    {
        if (rawText) {
            return parsed().isScalar();
        }
        return itemTag(slice, offset) == ItemTag.TYPED_VALUE;
    }

    @Override
    public boolean isError()
    {
        if (rawText) {
            return parsed().isError();
        }
        return itemTag(slice, offset) == ItemTag.JSON_ERROR;
    }

    // --- ARRAY ---

    @Override
    public int arraySize()
    {
        if (rawText) {
            return parsed().arraySize();
        }
        return JsonItemEncoding.arraySize(slice, offset);
    }

    @Override
    public Json arrayElement(int index)
    {
        if (rawText) {
            return parsed().arrayElement(index);
        }
        int size = arraySize();
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds for size " + size);
        }
        return switch (itemTag(slice, offset)) {
            case ARRAY -> {
                int cursor = arrayItemsStart(slice, offset);
                for (int i = 0; i < index; i++) {
                    cursor = itemEndOffset(slice, cursor);
                }
                yield view(slice, cursor, itemEndOffset(slice, cursor));
            }
            case ARRAY_INDEXED -> {
                int childOffset = arrayIndexedElementOffset(slice, offset, index);
                yield view(slice, childOffset, itemEndOffset(slice, childOffset));
            }
            default -> throw new IllegalStateException("Not an ARRAY");
        };
    }

    @Override
    public void forEachArrayElement(Consumer<Json> consumer)
    {
        requireNonNull(consumer, "consumer is null");
        if (rawText) {
            parsed().forEachArrayElement(consumer);
            return;
        }
        int count = arraySize();
        int cursor = arrayItemsStart(slice, offset);
        for (int i = 0; i < count; i++) {
            int next = itemEndOffset(slice, cursor);
            consumer.accept(view(slice, cursor, next));
            cursor = next;
        }
    }

    @Override
    public boolean anyArrayElement(Predicate<Json> predicate)
    {
        requireNonNull(predicate, "predicate is null");
        if (rawText) {
            return parsed().anyArrayElement(predicate);
        }
        int count = arraySize();
        int cursor = arrayItemsStart(slice, offset);
        for (int i = 0; i < count; i++) {
            int next = itemEndOffset(slice, cursor);
            if (predicate.test(view(slice, cursor, next))) {
                return true;
            }
            cursor = next;
        }
        return false;
    }

    // --- OBJECT ---

    @Override
    public int objectSize()
    {
        if (rawText) {
            return parsed().objectSize();
        }
        return JsonItemEncoding.objectSize(slice, offset);
    }

    @Override
    public Optional<Json> objectMember(Slice keyBytes)
    {
        requireNonNull(keyBytes, "keyBytes is null");
        if (rawText) {
            return parsed().objectMember(keyBytes);
        }
        return switch (itemTag(slice, offset)) {
            case OBJECT -> linearSearchObjectMember(keyBytes);
            case OBJECT_INDEXED -> indexedSearchObjectMember(keyBytes);
            default -> throw new IllegalStateException("Not an OBJECT");
        };
    }

    @Override
    public void objectMembers(Slice keyBytes, Consumer<Json> consumer)
    {
        requireNonNull(keyBytes, "keyBytes is null");
        requireNonNull(consumer, "consumer is null");
        if (rawText) {
            parsed().objectMembers(keyBytes, consumer);
            return;
        }
        switch (itemTag(slice, offset)) {
            case OBJECT -> linearScanObjectMembers(keyBytes, consumer);
            case OBJECT_INDEXED -> indexedScanObjectMembers(keyBytes, consumer);
            default -> throw new IllegalStateException("Not an OBJECT");
        }
    }

    @Override
    public void forEachObjectMember(BiConsumer<String, Json> consumer)
    {
        requireNonNull(consumer, "consumer is null");
        if (rawText) {
            parsed().forEachObjectMember(consumer);
            return;
        }
        int count = objectSize();
        ItemTag tag = itemTag(slice, offset);
        if (tag == ItemTag.OBJECT) {
            int cursor = objectEntriesStart(slice, offset);
            for (int i = 0; i < count; i++) {
                String memberKey = readString(slice, cursor);
                int valueOffset = stringEndOffset(slice, cursor);
                int childEnd = itemEndOffset(slice, valueOffset);
                consumer.accept(memberKey, view(slice, valueOffset, childEnd));
                cursor = childEnd;
            }
        }
        else if (tag == ItemTag.OBJECT_INDEXED) {
            for (int i = 0; i < count; i++) {
                int entryOffset = objectIndexedEntryOffset(slice, offset, i);
                String memberKey = readString(slice, entryOffset);
                int valueOffset = stringEndOffset(slice, entryOffset);
                int childEnd = objectIndexedEntryEnd(slice, offset, i);
                consumer.accept(memberKey, view(slice, valueOffset, childEnd));
            }
        }
        else {
            throw new IllegalStateException("Not an OBJECT");
        }
    }

    @Override
    public void forEachObjectMemberBytes(BiConsumer<Slice, Json> consumer)
    {
        requireNonNull(consumer, "consumer is null");
        if (rawText) {
            parsed().forEachObjectMemberBytes(consumer);
            return;
        }
        int count = objectSize();
        ItemTag tag = itemTag(slice, offset);
        if (tag == ItemTag.OBJECT) {
            int cursor = objectEntriesStart(slice, offset);
            for (int i = 0; i < count; i++) {
                Slice memberKey = readStringSlice(slice, cursor);
                int valueOffset = stringEndOffset(slice, cursor);
                int childEnd = itemEndOffset(slice, valueOffset);
                consumer.accept(memberKey, view(slice, valueOffset, childEnd));
                cursor = childEnd;
            }
        }
        else if (tag == ItemTag.OBJECT_INDEXED) {
            for (int i = 0; i < count; i++) {
                int entryOffset = objectIndexedEntryOffset(slice, offset, i);
                Slice memberKey = readStringSlice(slice, entryOffset);
                int valueOffset = stringEndOffset(slice, entryOffset);
                int childEnd = objectIndexedEntryEnd(slice, offset, i);
                consumer.accept(memberKey, view(slice, valueOffset, childEnd));
            }
        }
        else {
            throw new IllegalStateException("Not an OBJECT");
        }
    }

    private Optional<Json> linearSearchObjectMember(Slice keySlice)
    {
        int count = objectSize();
        int cursor = objectEntriesStart(slice, offset);
        int keyLength = keySlice.length();
        for (int i = 0; i < count; i++) {
            int memberKeyLength = slice.getInt(cursor);
            int valueOffset = cursor + Integer.BYTES + memberKeyLength;
            int childEnd = itemEndOffset(slice, valueOffset);
            if (memberKeyLength == keyLength && slice.equals(cursor + Integer.BYTES, memberKeyLength, keySlice, 0, keyLength)) {
                return Optional.of(view(slice, valueOffset, childEnd));
            }
            cursor = childEnd;
        }
        return Optional.empty();
    }

    private Optional<Json> indexedSearchObjectMember(Slice keySlice)
    {
        int firstSorted = findFirstSortedIndex(keySlice);
        if (firstSorted < 0) {
            return Optional.empty();
        }
        int entryIndex = objectIndexedSorting(slice, offset, firstSorted);
        int entryOffset = objectIndexedEntryOffset(slice, offset, entryIndex);
        int valueOffset = stringEndOffset(slice, entryOffset);
        int childEnd = objectIndexedEntryEnd(slice, offset, entryIndex);
        return Optional.of(view(slice, valueOffset, childEnd));
    }

    private void linearScanObjectMembers(Slice keySlice, Consumer<Json> consumer)
    {
        int count = objectSize();
        int cursor = objectEntriesStart(slice, offset);
        int keyLength = keySlice.length();
        for (int i = 0; i < count; i++) {
            int memberKeyLength = slice.getInt(cursor);
            int valueOffset = cursor + Integer.BYTES + memberKeyLength;
            int childEnd = itemEndOffset(slice, valueOffset);
            if (memberKeyLength == keyLength && slice.equals(cursor + Integer.BYTES, memberKeyLength, keySlice, 0, keyLength)) {
                consumer.accept(view(slice, valueOffset, childEnd));
            }
            cursor = childEnd;
        }
    }

    private void indexedScanObjectMembers(Slice keySlice, Consumer<Json> consumer)
    {
        int firstSorted = findFirstSortedIndex(keySlice);
        if (firstSorted < 0) {
            return;
        }
        int count = objectSize();
        int keyLength = keySlice.length();
        for (int i = firstSorted; i < count; i++) {
            int entryIndex = objectIndexedSorting(slice, offset, i);
            int entryOffset = objectIndexedEntryOffset(slice, offset, entryIndex);
            int memberKeyLength = slice.getInt(entryOffset);
            if (memberKeyLength != keyLength || !slice.equals(entryOffset + Integer.BYTES, memberKeyLength, keySlice, 0, keyLength)) {
                return;
            }
            int valueOffset = entryOffset + Integer.BYTES + memberKeyLength;
            int childEnd = objectIndexedEntryEnd(slice, offset, entryIndex);
            consumer.accept(view(slice, valueOffset, childEnd));
        }
    }

    private int findFirstSortedIndex(Slice targetSlice)
    {
        int count = objectSize();
        int low = 0;
        int high = count - 1;
        int found = -1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int entryIndex = objectIndexedSorting(slice, offset, mid);
            int entryOffset = objectIndexedEntryOffset(slice, offset, entryIndex);
            Slice keySlice = readStringSlice(slice, entryOffset);
            int cmp = keySlice.compareTo(targetSlice);
            if (cmp < 0) {
                low = mid + 1;
            }
            else if (cmp > 0) {
                high = mid - 1;
            }
            else {
                found = mid;
                high = mid - 1;
            }
        }
        return found;
    }

    // --- SCALAR ---

    @Override
    public TypeTag scalarType()
    {
        if (rawText) {
            return parsed().scalarType();
        }
        if (itemTag(slice, offset) != ItemTag.TYPED_VALUE) {
            throw new IllegalStateException("Not a TYPED_VALUE scalar");
        }
        return TypeTag.fromEncoded(slice.getByte(offset + Byte.BYTES));
    }

    @Override
    public TypedValue materializeScalar()
    {
        if (rawText) {
            return parsed().materializeScalar();
        }
        return JsonItemEncoding.readTypedValue(slice, offset);
    }

    // --- raw access ---

    @Override
    public Slice encoding()
    {
        if (rawText) {
            return parsed().encoding();
        }
        return copyItemEncoding(slice, offset, end);
    }

    @Override
    public Slice backingSlice()
    {
        if (rawText) {
            return parsed().backingSlice();
        }
        return slice;
    }

    @Override
    public int viewOffset()
    {
        if (rawText) {
            return parsed().viewOffset();
        }
        return offset;
    }

    @Override
    public int viewEnd()
    {
        if (rawText) {
            return parsed().viewEnd();
        }
        return end;
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
        if (rawText) {
            return "Json[RAW, " + slice.length() + " bytes]";
        }
        return "Json[" + kind() + ", " + (end - offset) + " bytes]";
    }
}
