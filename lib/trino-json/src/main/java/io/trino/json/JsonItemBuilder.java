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

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TrinoNumber;
import it.unimi.dsi.fastutil.ints.IntArrays;

import java.util.Arrays;
import java.util.function.Consumer;

import static io.airlift.slice.Slices.utf8Slice;

/// Streaming, single-buffer encoder for typed-item JSON values.
///
/// All writes go to one [DynamicSliceOutput]. Containers (ARRAY / OBJECT) are emitted with
/// a placeholder element-count word that gets patched on `endArray` / `endObject`. There
/// are no intermediate per-subtree slice allocations: a deeply nested document costs
/// exactly one buffer.
///
/// Two ways to use it.
///
/// **Lambda style** — structurally safe (containers can't be left open or unbalanced):
/// ```
/// Json item = JsonItemBuilder.encodeArray(a -> a
///         .bigint(1)
///         .nullValue()
///         .object(o -> o.varchar("k", utf8Slice("v"))));
/// ```
///
/// **Stateful writer** — flexible, used internally and where lambdas don't fit:
/// ```
/// JsonItemBuilder.encode(w -> w
///         .startArray()
///             .bigint(1)
///             .nullValue()
///             .startObject()
///                 .fieldName("k").varchar(utf8Slice("v"))
///             .endObject()
///         .endArray());
/// ```
///
/// Misuse (mismatched start/end, value where field name is expected, more than one root
/// item, unfinished container at finish) throws [IllegalStateException] on the offending
/// call.
public final class JsonItemBuilder
{
    /// Constant `Json` for JSON null. Wire form: VERSION + JSON_NULL tag.
    public static final Json JSON_NULL = JsonNullValue.INSTANCE;

    /// Constant `Json` for the JSON_ERROR sentinel.
    public static final Json JSON_ERROR = JsonErrorValue.INSTANCE;

    private JsonItemBuilder() {}

    /// Encode a single root item. The lambda must write exactly one value.
    public static Json encode(Consumer<JsonItemWriter> body)
    {
        return encode(body, 64);
    }

    /// Encode a single root item with a pre-sized output buffer. `expectedSize` is the
    /// expected total length (including the VERSION byte); callers that pre-walk a
    /// tree-form value can pass the exact size to skip realloc-and-copy passes during
    /// growth. The buffer still grows if writes exceed the hint, so a wrong estimate is
    /// safe (just slightly slower).
    public static Json encode(Consumer<JsonItemWriter> body, int expectedSize)
    {
        DynamicSliceOutput output = new DynamicSliceOutput(Math.max(expectedSize, 1));
        JsonItemEncoding.appendVersion(output);
        JsonItemWriter writer = new JsonItemWriter(output);
        body.accept(writer);
        return writer.finish();
    }

    /// Encodes a root item directly into `output`, appending to whatever is already there. Used
    /// where the destination buffer already exists — a block builder writing a value into the
    /// column's buffer — so no intermediate slice is allocated for the value.
    public static void encodeInto(DynamicSliceOutput output, Consumer<JsonItemWriter> body)
    {
        JsonItemEncoding.appendVersion(output);
        JsonItemWriter writer = new JsonItemWriter(output);
        body.accept(writer);
        writer.finishInPlace();
    }

    /// Convenience: encode an array as the root item.
    public static Json encodeArray(Consumer<ArrayWriter> body)
    {
        return encode(w -> {
            w.startArray();
            body.accept(new ArrayWriter(w));
            w.endArray();
        });
    }

    /// Convenience: encode an object as the root item.
    public static Json encodeObject(Consumer<ObjectWriter> body)
    {
        return encode(w -> {
            w.startObject();
            body.accept(new ObjectWriter(w));
            w.endObject();
        });
    }

    public static Json encodeBoolean(boolean value)
    {
        return encode(w -> w.booleanValue(value));
    }

    public static Json encodeBigint(long value)
    {
        return encode(w -> w.bigint(value));
    }

    public static Json encodeDouble(double value)
    {
        return encode(w -> w.doubleValue(value));
    }

    public static Json encodeVarchar(Slice value)
    {
        return encode(w -> w.varchar(value));
    }

    public static Json encodeShortDecimal(int precision, int scale, long value)
    {
        return encode(w -> w.shortDecimal(precision, scale, value));
    }

    public static Json encodeLongDecimal(int precision, int scale, Int128 value)
    {
        return encode(w -> w.longDecimal(precision, scale, value));
    }

    public static Json encodeDate(int days)
    {
        return encode(w -> w.date(days));
    }

    public static Json encodeTime(int precision, long picosOfDay)
    {
        return encode(w -> w.time(precision, picosOfDay));
    }

    public static Json encodeTimeWithTimeZone(int precision, long picosOfDay, int offsetMinutes)
    {
        return encode(w -> w.timeWithTimeZone(precision, picosOfDay, offsetMinutes));
    }

    public static Json encodeTimestamp(int precision, long epochMicros, int picosOfMicro)
    {
        return encode(w -> w.timestamp(precision, epochMicros, picosOfMicro));
    }

    public static Json encodeTimestampWithTimeZone(int precision, long epochMillis, int picosOfMilli, short timeZoneKey)
    {
        return encode(w -> w.timestampWithTimeZone(precision, epochMillis, picosOfMilli, timeZoneKey));
    }

    public static Json encodeNumber(TrinoNumber value)
    {
        return encode(w -> w.numberValue(value));
    }

    /// Stateful streaming writer. Tracks open containers and validates calls.
    public static final class JsonItemWriter
    {
        private static final int CTX_ROOT = 0;
        private static final int CTX_ARRAY = 1;
        private static final int CTX_OBJECT_FIELD = 2;
        private static final int CTX_OBJECT_VALUE = 3;
        private static final int CTX_INDEXED_ARRAY = 4;
        private static final int CTX_INDEXED_OBJECT_FIELD = 5;
        private static final int CTX_INDEXED_OBJECT_VALUE = 6;

        private final DynamicSliceOutput mainOutput;
        // Currently-active output. Swapped to per-frame scratch buffers while an
        // ARRAY_INDEXED / OBJECT_INDEXED container is open (we can't write the
        // offsets table until the child count and per-child positions are known),
        // restored on endIndexedArray / endIndexedObject.
        private DynamicSliceOutput output;
        // Tag offset of each open container (the byte position of its tag). Both the
        // size and count words are at fixed offsets relative to this — patched together
        // when the container closes. Indexed frames don't use this slot (set to -1)
        // because the tag isn't written until close.
        private int[] tagOffsets = new int[8];
        private int[] counts = new int[8];
        private int[] states = new int[8];
        // Per-indexed-frame state, packed by depth (only valid for indexed frames).
        // `frames[depth]` is null for non-indexed depths.
        private IndexedFrame[] frames = new IndexedFrame[8];
        private int depth;
        private boolean rootDone;

        private int[] patches = new int[16];
        private int patchSize;

        JsonItemWriter(DynamicSliceOutput output)
        {
            this.mainOutput = output;
            this.output = output;
            states[0] = CTX_ROOT;
        }

        public JsonItemWriter startArray()
        {
            beforeValue();
            int offset = JsonItemEncoding.appendArrayItemPlaceholder(output);
            push(CTX_ARRAY, offset, null);
            return this;
        }

        public JsonItemWriter endArray()
        {
            checkState(states[depth] == CTX_ARRAY, "endArray called outside array");
            popAndPatch();
            afterValue();
            return this;
        }

        public JsonItemWriter startObject()
        {
            beforeValue();
            int offset = JsonItemEncoding.appendObjectItemPlaceholder(output);
            push(CTX_OBJECT_FIELD, offset, null);
            return this;
        }

        public JsonItemWriter endObject()
        {
            checkState(states[depth] == CTX_OBJECT_FIELD, "endObject called expecting %s", stateName(states[depth]));
            popAndPatch();
            afterValue();
            return this;
        }

        /// Begin an `ARRAY_INDEXED` container. Children are written into a per-frame
        /// scratch buffer; the offsets table is prepended at [#endIndexedArray] once
        /// the child count is known.
        public JsonItemWriter startIndexedArray()
        {
            beforeValue();
            IndexedFrame frame = new IndexedFrame(output, patchSize, false);
            output = frame.scratch;
            push(CTX_INDEXED_ARRAY, -1, frame);
            return this;
        }

        public JsonItemWriter endIndexedArray()
        {
            checkState(states[depth] == CTX_INDEXED_ARRAY, "endIndexedArray called outside indexed array");
            IndexedFrame frame = frames[depth];
            int count = counts[depth];
            applyFramePatches(frame);
            output = frame.parentOutput;
            int[] offsets = Arrays.copyOf(frame.offsets, count + 1);
            offsets[count] = frame.scratch.size();
            JsonItemEncoding.appendArrayIndexed(output, offsets, frame.scratch.slice());
            popFrame();
            afterValue();
            return this;
        }

        /// Begin an `OBJECT_INDEXED` container. Children are written into a per-frame
        /// scratch buffer; the sort permutation and offsets table are prepended at
        /// [#endIndexedObject] once the entries are known.
        public JsonItemWriter startIndexedObject()
        {
            beforeValue();
            IndexedFrame frame = new IndexedFrame(output, patchSize, true);
            output = frame.scratch;
            push(CTX_INDEXED_OBJECT_FIELD, -1, frame);
            return this;
        }

        public JsonItemWriter endIndexedObject()
        {
            checkState(states[depth] == CTX_INDEXED_OBJECT_FIELD, "endIndexedObject called expecting %s", stateName(states[depth]));
            IndexedFrame frame = frames[depth];
            int count = counts[depth];
            applyFramePatches(frame);
            output = frame.parentOutput;
            int[] offsets = Arrays.copyOf(frame.offsets, count + 1);
            offsets[count] = frame.scratch.size();
            short[] sortPermutation = sortPermutation(frame.keys, count);
            JsonItemEncoding.appendObjectIndexed(output, count, sortPermutation, offsets, frame.scratch.slice());
            popFrame();
            afterValue();
            return this;
        }

        public JsonItemWriter fieldName(Slice key)
        {
            int state = states[depth];
            checkState(state == CTX_OBJECT_FIELD || state == CTX_INDEXED_OBJECT_FIELD,
                    "fieldName called outside object (state=%s)",
                    stateName(state));
            if (state == CTX_INDEXED_OBJECT_FIELD) {
                IndexedFrame frame = frames[depth];
                frame.recordOffset(counts[depth], output.size());
                frame.recordKey(counts[depth], key);
            }
            JsonItemEncoding.appendObjectKey(output, key);
            states[depth] = (state == CTX_INDEXED_OBJECT_FIELD) ? CTX_INDEXED_OBJECT_VALUE : CTX_OBJECT_VALUE;
            return this;
        }

        public JsonItemWriter fieldName(String key)
        {
            int state = states[depth];
            checkState(state == CTX_OBJECT_FIELD || state == CTX_INDEXED_OBJECT_FIELD,
                    "fieldName called outside object (state=%s)",
                    stateName(state));
            if (state == CTX_INDEXED_OBJECT_FIELD) {
                // OBJECT_INDEXED entries are (length-prefixed key + item value). The
                // offsets table points at the entry start (= where the key begins),
                // so record the position *before* the key is appended; recordKey
                // captures the key bytes for the sort permutation built at
                // endIndexedObject.
                IndexedFrame frame = frames[depth];
                frame.recordOffset(counts[depth], output.size());
                frame.recordKey(counts[depth], utf8Slice(key));
            }
            JsonItemEncoding.appendObjectKey(output, key);
            states[depth] = (state == CTX_INDEXED_OBJECT_FIELD) ? CTX_INDEXED_OBJECT_VALUE : CTX_OBJECT_VALUE;
            return this;
        }

        public JsonItemWriter nullValue()
        {
            beforeValue();
            JsonItemEncoding.appendJsonNullItem(output);
            afterValue();
            return this;
        }

        public JsonItemWriter errorValue()
        {
            beforeValue();
            JsonItemEncoding.appendJsonErrorItem(output);
            afterValue();
            return this;
        }

        public JsonItemWriter booleanValue(boolean value)
        {
            beforeValue();
            JsonItemEncoding.appendBoolean(output, value);
            afterValue();
            return this;
        }

        public JsonItemWriter bigint(long value)
        {
            beforeValue();
            JsonItemEncoding.appendBigint(output, value);
            afterValue();
            return this;
        }

        public JsonItemWriter integerValue(long value)
        {
            beforeValue();
            JsonItemEncoding.appendInteger(output, value);
            afterValue();
            return this;
        }

        public JsonItemWriter smallintValue(long value)
        {
            beforeValue();
            JsonItemEncoding.appendSmallint(output, value);
            afterValue();
            return this;
        }

        public JsonItemWriter tinyintValue(long value)
        {
            beforeValue();
            JsonItemEncoding.appendTinyint(output, value);
            afterValue();
            return this;
        }

        public JsonItemWriter doubleValue(double value)
        {
            beforeValue();
            JsonItemEncoding.appendDouble(output, value);
            afterValue();
            return this;
        }

        public JsonItemWriter realBits(int bits)
        {
            beforeValue();
            JsonItemEncoding.appendRealBits(output, bits);
            afterValue();
            return this;
        }

        public JsonItemWriter varchar(Slice value)
        {
            beforeValue();
            JsonItemEncoding.appendVarchar(output, value);
            afterValue();
            return this;
        }

        public JsonItemWriter date(int days)
        {
            beforeValue();
            JsonItemEncoding.appendDate(output, days);
            afterValue();
            return this;
        }

        public JsonItemWriter time(int precision, long picosOfDay)
        {
            beforeValue();
            JsonItemEncoding.appendTime(output, precision, picosOfDay);
            afterValue();
            return this;
        }

        public JsonItemWriter timeWithTimeZone(int precision, long picosOfDay, int offsetMinutes)
        {
            beforeValue();
            JsonItemEncoding.appendTimeWithTimeZone(output, precision, picosOfDay, offsetMinutes);
            afterValue();
            return this;
        }

        public JsonItemWriter timestamp(int precision, long epochMicros, int picosOfMicro)
        {
            beforeValue();
            JsonItemEncoding.appendTimestamp(output, precision, epochMicros, picosOfMicro);
            afterValue();
            return this;
        }

        public JsonItemWriter timestampWithTimeZone(int precision, long epochMillis, int picosOfMilli, short timeZoneKey)
        {
            beforeValue();
            JsonItemEncoding.appendTimestampWithTimeZone(output, precision, epochMillis, picosOfMilli, timeZoneKey);
            afterValue();
            return this;
        }

        public JsonItemWriter shortDecimal(int precision, int scale, long value)
        {
            beforeValue();
            JsonItemEncoding.appendShortDecimal(output, precision, scale, value);
            afterValue();
            return this;
        }

        public JsonItemWriter longDecimal(int precision, int scale, Int128 value)
        {
            beforeValue();
            JsonItemEncoding.appendLongDecimal(output, precision, scale, value);
            afterValue();
            return this;
        }

        public JsonItemWriter numberValue(TrinoNumber value)
        {
            beforeValue();
            JsonItemEncoding.appendNumber(output, value);
            afterValue();
            return this;
        }

        /// Embeds an existing encoded item. The only path that copies bytes; unavoidable
        /// when the source already lives in its own slice.
        public JsonItemWriter nest(Json item)
        {
            beforeValue();
            JsonItemEncoding.appendNestedItem(output, item.encoding());
            afterValue();
            return this;
        }

        /// Applies the pending patches without wrapping the result: the bytes were written into a
        /// buffer the caller owns, and the item is addressed by its offsets there. Patch positions
        /// are absolute in that buffer, so this works whether the item started at 0 or not.
        void finishInPlace()
        {
            checkState(depth == 0, "finish called with %s open container(s)", depth);
            checkState(rootDone, "finish called with no root item written");
            Slice slice = mainOutput.slice();
            for (int i = 0; i < patchSize; i += 2) {
                slice.setInt(patches[i], patches[i + 1]);
            }
        }

        Json finish()
        {
            checkState(depth == 0, "finish called with %s open container(s)", depth);
            checkState(rootDone, "finish called with no root item written");
            // Patches (container size/count words) are applied here against the final slice
            // rather than inline at popAndPatch time. DynamicSliceOutput's backing array may
            // grow (and be copied to a new array) between popping a container and the next
            // write, so an inline setInt would write to a stale buffer. Batching at finish
            // guarantees the slice we patch is the one returned to the caller.
            Slice slice = mainOutput.slice();
            for (int i = 0; i < patchSize; i += 2) {
                slice.setInt(patches[i], patches[i + 1]);
            }
            return Json.of(slice);
        }

        private void beforeValue()
        {
            int state = states[depth];
            if (state == CTX_ROOT) {
                checkState(!rootDone, "more than one root item written");
                return;
            }
            if (state == CTX_INDEXED_ARRAY) {
                // Record the start offset of this array element inside the scratch
                // buffer before any bytes are written. The closing offsets table is
                // built from these positions. (Indexed object entries record their
                // offset in fieldName so the table points at the key, not the value.)
                frames[depth].recordOffset(counts[depth], output.size());
                return;
            }
            if (state == CTX_INDEXED_OBJECT_VALUE) {
                return;
            }
            if (state == CTX_ARRAY || state == CTX_OBJECT_VALUE) {
                return;
            }
            throw new IllegalStateException("value written where %s was expected".formatted(stateName(state)));
        }

        private void afterValue()
        {
            int state = states[depth];
            if (state == CTX_ROOT) {
                rootDone = true;
                return;
            }
            counts[depth]++;
            if (state == CTX_OBJECT_VALUE) {
                states[depth] = CTX_OBJECT_FIELD;
            }
            else if (state == CTX_INDEXED_OBJECT_VALUE) {
                states[depth] = CTX_INDEXED_OBJECT_FIELD;
            }
        }

        private void push(int newState, int tagOffset, IndexedFrame frame)
        {
            depth++;
            ensureCapacity(depth + 1);
            states[depth] = newState;
            counts[depth] = 0;
            tagOffsets[depth] = tagOffset;
            frames[depth] = frame;
        }

        private void popAndPatch()
        {
            int tagOffset = tagOffsets[depth];
            int containerSize = output.size() - tagOffset;
            // Two patches per container: total byte size, then element count.
            addPatch(JsonItemEncoding.containerSizeOffset(tagOffset), containerSize);
            addPatch(JsonItemEncoding.containerCountOffset(tagOffset), counts[depth]);
            depth--;
        }

        private void popFrame()
        {
            frames[depth] = null;
            depth--;
        }

        // Apply (and consume) patches recorded while writing into the indexed frame's
        // scratch. They reference scratch-relative offsets and must be written before
        // the scratch is copied into the parent output.
        private void applyFramePatches(IndexedFrame frame)
        {
            Slice scratchSlice = frame.scratch.slice();
            for (int i = frame.patchWatermark; i < patchSize; i += 2) {
                scratchSlice.setInt(patches[i], patches[i + 1]);
            }
            patchSize = frame.patchWatermark;
        }

        private void addPatch(int offset, int value)
        {
            if (patchSize + 2 > patches.length) {
                patches = Arrays.copyOf(patches, patches.length * 2);
            }
            patches[patchSize++] = offset;
            patches[patchSize++] = value;
        }

        private void ensureCapacity(int needed)
        {
            if (states.length >= needed) {
                return;
            }
            int newSize = Math.max(states.length * 2, needed);
            states = Arrays.copyOf(states, newSize);
            counts = Arrays.copyOf(counts, newSize);
            tagOffsets = Arrays.copyOf(tagOffsets, newSize);
            frames = Arrays.copyOf(frames, newSize);
        }

        // Sorts entryOrder[0..count) by lex order of keys[i] and returns the
        // resulting permutation as int16 entries. No `Integer` boxing.
        private static short[] sortPermutation(Slice[] keys, int count)
        {
            int[] entryOrder = new int[count];
            for (int i = 0; i < count; i++) {
                entryOrder[i] = i;
            }
            IntArrays.unstableSort(entryOrder, 0, count, (a, b) -> keys[a].compareTo(keys[b]));
            short[] sortPermutation = new short[count];
            for (int i = 0; i < count; i++) {
                sortPermutation[i] = (short) entryOrder[i];
            }
            return sortPermutation;
        }

        private static String stateName(int state)
        {
            return switch (state) {
                case CTX_ROOT -> "root";
                case CTX_ARRAY -> "array element";
                case CTX_INDEXED_ARRAY -> "indexed array element";
                case CTX_OBJECT_FIELD -> "object field name";
                case CTX_OBJECT_VALUE -> "object value";
                case CTX_INDEXED_OBJECT_FIELD -> "indexed object field name";
                case CTX_INDEXED_OBJECT_VALUE -> "indexed object value";
                default -> "?";
            };
        }

        @FormatMethod
        private static void checkState(boolean condition, @FormatString String message, Object... args)
        {
            if (!condition) {
                throw new IllegalStateException(message.formatted(args));
            }
        }
    }

    /// Per-indexed-frame state held in [JsonItemWriter#frames]. Each open
    /// `ARRAY_INDEXED` / `OBJECT_INDEXED` container writes its child items into
    /// its own scratch [DynamicSliceOutput]; the parent's offsets table (and the
    /// object's sort permutation) is computed at close from the entry positions
    /// captured here. Patches recorded while the frame is open reference
    /// scratch-relative offsets, so they're flushed against the scratch slice
    /// before it gets copied into the parent output.
    private static final class IndexedFrame
    {
        final DynamicSliceOutput parentOutput;
        final DynamicSliceOutput scratch;
        final int patchWatermark;
        int[] offsets = new int[8];
        Slice[] keys;

        IndexedFrame(DynamicSliceOutput parentOutput, int patchWatermark, boolean object)
        {
            this.parentOutput = parentOutput;
            this.patchWatermark = patchWatermark;
            this.scratch = new DynamicSliceOutput(64);
            if (object) {
                this.keys = new Slice[8];
            }
        }

        void recordOffset(int index, int offset)
        {
            if (index >= offsets.length) {
                offsets = Arrays.copyOf(offsets, Math.max(offsets.length * 2, index + 1));
            }
            offsets[index] = offset;
        }

        void recordKey(int index, Slice key)
        {
            if (index >= keys.length) {
                keys = Arrays.copyOf(keys, Math.max(keys.length * 2, index + 1));
            }
            keys[index] = key;
        }
    }

    /// Lambda-context for an open ARRAY. Each call appends one element to the surrounding
    /// array; nested containers are encoded directly into the same buffer.
    public static final class ArrayWriter
    {
        private final JsonItemWriter writer;

        ArrayWriter(JsonItemWriter writer)
        {
            this.writer = writer;
        }

        public ArrayWriter nullValue()
        {
            writer.nullValue();
            return this;
        }

        public ArrayWriter booleanValue(boolean value)
        {
            writer.booleanValue(value);
            return this;
        }

        public ArrayWriter bigint(long value)
        {
            writer.bigint(value);
            return this;
        }

        public ArrayWriter doubleValue(double value)
        {
            writer.doubleValue(value);
            return this;
        }

        public ArrayWriter varchar(Slice value)
        {
            writer.varchar(value);
            return this;
        }

        public ArrayWriter array(Consumer<ArrayWriter> body)
        {
            writer.startArray();
            body.accept(new ArrayWriter(writer));
            writer.endArray();
            return this;
        }

        public ArrayWriter object(Consumer<ObjectWriter> body)
        {
            writer.startObject();
            body.accept(new ObjectWriter(writer));
            writer.endObject();
            return this;
        }

        public ArrayWriter nest(Json item)
        {
            writer.nest(item);
            return this;
        }
    }

    /// Lambda-context for an open OBJECT. Each call writes a field-name + value pair.
    public static final class ObjectWriter
    {
        private final JsonItemWriter writer;

        ObjectWriter(JsonItemWriter writer)
        {
            this.writer = writer;
        }

        public ObjectWriter nullValue(String key)
        {
            writer.fieldName(key).nullValue();
            return this;
        }

        public ObjectWriter booleanValue(String key, boolean value)
        {
            writer.fieldName(key).booleanValue(value);
            return this;
        }

        public ObjectWriter bigint(String key, long value)
        {
            writer.fieldName(key).bigint(value);
            return this;
        }

        public ObjectWriter doubleValue(String key, double value)
        {
            writer.fieldName(key).doubleValue(value);
            return this;
        }

        public ObjectWriter varchar(String key, Slice value)
        {
            writer.fieldName(key).varchar(value);
            return this;
        }

        public ObjectWriter array(String key, Consumer<ArrayWriter> body)
        {
            writer.fieldName(key).startArray();
            body.accept(new ArrayWriter(writer));
            writer.endArray();
            return this;
        }

        public ObjectWriter object(String key, Consumer<ObjectWriter> body)
        {
            writer.fieldName(key).startObject();
            body.accept(new ObjectWriter(writer));
            writer.endObject();
            return this;
        }

        public ObjectWriter nest(String key, Json item)
        {
            writer.fieldName(key).nest(item);
            return this;
        }

        public ObjectWriter nest(Slice key, Json item)
        {
            writer.fieldName(key).nest(item);
            return this;
        }
    }
}
