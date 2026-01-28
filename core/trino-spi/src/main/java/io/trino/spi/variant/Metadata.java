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
package io.trino.spi.variant;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntUnaryOperator;

import static io.trino.spi.variant.Header.metadataHeader;
import static io.trino.spi.variant.Header.metadataIsSorted;
import static io.trino.spi.variant.Header.metadataOffsetSize;
import static io.trino.spi.variant.Header.metadataVersion;
import static io.trino.spi.variant.VariantUtils.checkArgument;
import static io.trino.spi.variant.VariantUtils.checkState;
import static io.trino.spi.variant.VariantUtils.getOffsetSize;
import static io.trino.spi.variant.VariantUtils.readOffset;
import static io.trino.spi.variant.VariantUtils.writeOffset;
import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

public final class Metadata
{
    public static final Slice EMPTY_METADATA_SLICE = Slices.wrappedBuffer(metadataHeader(false, 1), (byte) 0, (byte) 0);
    public static final Metadata EMPTY_METADATA = new Metadata(EMPTY_METADATA_SLICE, false, 0, 1);

    private final Slice metadata;
    private final boolean sorted;
    private final int dictionarySize;
    private final int offsetSize;

    private Metadata(Slice metadata, boolean sorted, int dictionarySize, int offsetSize)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sorted = sorted;
        checkArgument(dictionarySize >= 0, "dictionarySize is negative");
        checkArgument(offsetSize >= 1 && offsetSize <= 4, () -> "offsetSize is out of range: %s" + offsetSize);
        this.dictionarySize = dictionarySize;
        this.offsetSize = offsetSize;
    }

    public static Metadata from(Slice metadata)
    {
        if (metadata == EMPTY_METADATA_SLICE) {
            return EMPTY_METADATA;
        }

        // Quick validations
        checkArgument(metadata.length() >= 3, "metadata is empty");

        // Basic header/version check
        byte header = metadata.getByte(0);
        int version = metadataVersion(header);
        checkArgument(version == Header.VERSION, () -> "Unsupported metadata version: %s" + version);

        // Minimal structural check:
        // - we can read dictionarySize
        // - the implied dictionary region is within the slice bounds
        int offsetSize = metadataOffsetSize(header);
        checkArgument(metadata.length() >= 1 + offsetSize, "metadata is too short for dictionary size");

        int dictionarySize = readOffset(metadata, 1, offsetSize);
        checkArgument(dictionarySize >= 0, "Negative dictionary size: %s" + dictionarySize);

        // compute dictionaryStart = offsetsBase + (dictionarySize + 1) * offsetSize
        long dictionaryStart = 1 + (long) offsetSize + (long) (dictionarySize + 1) * offsetSize;
        checkArgument(dictionaryStart <= metadata.length(), "metadata is too short for dictionary offsets");

        // At this point:
        // - header is valid
        // - version matches
        // - offsets array and dictionary region fit in the slice
        //
        // NOT validated here (but validated when created):
        // - offsets[0] == 0
        // - offsets are non-decreasing
        // - last offset == dictionary length

        if (dictionarySize == 0) {
            metadata = EMPTY_METADATA_SLICE;
        }
        return new Metadata(metadata, metadataIsSorted(header), dictionarySize, offsetSize);
    }

    public static Metadata of(Collection<Slice> fieldNames)
    {
        requireNonNull(fieldNames, "fieldNames is null");
        if (fieldNames.isEmpty()) {
            return EMPTY_METADATA;
        }

        int dictionarySize = fieldNames.size();

        // Compute total dictionary length
        int dictionaryLength = 0;
        for (Slice fieldName : fieldNames) {
            dictionaryLength += fieldName.length();
        }

        boolean sorted = VariantUtils.isSorted(fieldNames);

        int offsetSize = getOffsetSize(dictionaryLength);

        // Layout:
        // [ header(1) ]
        // [ dictionarySize (offsetSize bytes) ]
        // [ (dictionarySize + 1) offsets (each offsetSize bytes) ]
        // [ dictionary bytes (dictionaryLength bytes) ]
        int headerAndSizeBytes = 1 + offsetSize;
        int offsetsBytes = (dictionarySize + 1) * offsetSize;
        int dictionaryStart = headerAndSizeBytes + offsetsBytes;
        int totalSize = dictionaryStart + dictionaryLength;

        Slice metadata = Slices.allocate(totalSize);
        int position = 0;

        // Header
        metadata.setByte(position, metadataHeader(sorted, offsetSize));
        position += 1;

        // Dictionary size
        writeOffset(metadata, position, dictionarySize, offsetSize);
        position += offsetSize;

        // Dictionary offsets (relative to start of dictionary data)
        int currentOffset = 0;

        // The first offset is always 0
        writeOffset(metadata, position, currentOffset, offsetSize);
        position += offsetSize;

        // Subsequent offsets are cumulative lengths
        for (Slice fieldName : fieldNames) {
            currentOffset += fieldName.length();
            writeOffset(metadata, position, currentOffset, offsetSize);
            position += offsetSize;
        }

        // Dictionary data: write directly into the final metadata slice
        int dictionaryPosition = dictionaryStart;
        for (Slice fieldName : fieldNames) {
            metadata.setBytes(dictionaryPosition, fieldName);
            dictionaryPosition += fieldName.length();
        }

        return new Metadata(metadata, sorted, dictionarySize, offsetSize);
    }

    public boolean isEmpty()
    {
        return metadata == EMPTY_METADATA_SLICE;
    }

    public boolean isSorted()
    {
        return sorted;
    }

    /// Returns the ID for a {@code name} in the dictionary, or -1 if not present.
    public int id(Slice name)
    {
        requireNonNull(name, "name is null");
        if (dictionarySize == 0) {
            return -1;
        }

        int offsetsBase = 1 + offsetSize;
        int dictionaryStart = offsetsBase + (dictionarySize + 1) * offsetSize;

        if (sorted && dictionarySize >= VariantUtils.BINARY_SEARCH_THRESHOLD) {
            return binarySearchIds(offsetsBase, offsetSize, dictionaryStart, dictionarySize, name);
        }
        return linearSearch(offsetsBase, offsetSize, dictionaryStart, dictionarySize, name);
    }

    private int linearSearch(
            int offsetsBase,
            int offsetSize,
            int dictionaryStart,
            int dictionarySize,
            Slice name)
    {
        int position = offsetsBase;

        // First offset (must be 0, already validated when created/loaded)
        int start = readOffset(metadata, position, offsetSize);
        position += offsetSize;

        for (int id = 0; id < dictionarySize; id++) {
            int end = readOffset(metadata, position, offsetSize);
            if (metadata.equals(dictionaryStart + start, end - start, name, 0, name.length())) {
                return id;
            }

            start = end;
            position += offsetSize;
        }

        return -1;
    }

    private int binarySearchIds(
            int offsetsBase,
            int offsetSize,
            int dictionaryStart,
            int dictionarySize,
            Slice target)
    {
        int low = 0;
        int high = dictionarySize - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            int midOffsetPos = offsetsBase + (mid * offsetSize);
            int start = readOffset(metadata, midOffsetPos, offsetSize);
            int end = readOffset(metadata, midOffsetPos + offsetSize, offsetSize);

            int compare = metadata.compareTo(dictionaryStart + start, end - start, target, 0, target.length());
            if (compare < 0) {
                low = mid + 1;
            }
            else if (compare > 0) {
                high = mid - 1;
            }
            else {
                return mid;
            }
        }

        return -1;
    }

    /// Returns the field name for an ID in metadata.
    ///
    /// @throws IndexOutOfBoundsException if the id is out of range
    public Slice get(int id)
    {
        checkIndex(id, dictionarySize);

        int offsetsBase = 1 + offsetSize;

        int offsetPosition = offsetsBase + (id * offsetSize);
        int start = readOffset(metadata, offsetPosition, offsetSize);
        int end = readOffset(metadata, offsetPosition + offsetSize, offsetSize);
        int length = end - start;

        // Offsets are relative to the start of the dictionary region
        int dictionaryStart = offsetsBase + (dictionarySize + 1) * offsetSize;
        return metadata.slice(dictionaryStart + start, length);
    }

    /// Returns the size of the metadata dictionary.
    public int dictionarySize()
    {
        return dictionarySize;
    }

    public void validateFully()
    {
        int position = 1 + offsetSize;
        int previous = -1;
        for (int i = 0; i < dictionarySize + 1; i++) {
            int value = readOffset(metadata, position, offsetSize);
            checkArgument(i != 0 || value == 0, "First dictionary offset must be 0");
            checkArgument(i == 0 || value > previous, "dictionary offsets must be strictly increasing");
            previous = value;
            position += offsetSize;
        }

        int dictionaryStart = 1 + offsetSize + (dictionarySize + 1) * offsetSize;
        int dictionaryLength = metadata.length() - dictionaryStart;
        checkArgument(previous == dictionaryLength, "Last dictionary offset must equal dictionary length");
    }

    public Slice toSlice()
    {
        return metadata;
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof Metadata that)) {
            return false;
        }
        return metadata.equals(that.metadata);
    }

    @Override
    public int hashCode()
    {
        return metadata.hashCode();
    }

    @Override
    public String toString()
    {
        return "Metadata[dictionarySize=%d, sorted=%s]".formatted(dictionarySize, sorted);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Map<Slice, Integer> nameToId = new HashMap<>();
        private List<Slice> names = new ArrayList<>();

        private Builder() {}

        /// Add a single field name and return the provisional fieldId.
        /// Final field-id must be resolved using the remap index returned by build().
        public int addFieldName(Slice fieldName)
        {
            checkNotBuilt();
            requireNonNull(fieldName, "fieldName is null");
            Integer existing = nameToId.get(fieldName);
            if (existing != null) {
                return existing;
            }

            int index = names.size();
            names.add(fieldName);
            nameToId.put(fieldName, index);
            return index;
        }

        /// Add multiple field names and return the provisional fieldIds.
        /// Final field-ids must be resolved using the remap index returned by build().
        public int[] addFieldNames(List<Slice> fieldNames)
        {
            checkNotBuilt();
            int[] provisionalFieldIds = new int[fieldNames.size()];
            for (int oldId = 0; oldId < fieldNames.size(); oldId++) {
                // final field-id may differ from this temporary index,
                // but the remap is in terms of final ids; we fix it in build().
                provisionalFieldIds[oldId] = addFieldName(fieldNames.get(oldId));
            }
            return provisionalFieldIds;
        }

        public int dictionarySize()
        {
            checkNotBuilt();
            return names.size();
        }

        /// Build final Metadata:
        /// - Field names sorted by UTF-8
        /// - Field-ids are 0..N-1 in that sorted order
        ///
        /// Also returns an optional remap from "builder index" -> final field-id,
        /// so you can fix any remap arrays that were filled with builder indices.
        public SortedMetadata buildSorted()
        {
            checkNotBuilt();
            if (names.isEmpty()) {
                Metadata empty = EMPTY_METADATA;
                return new SortedMetadata(empty, index -> {
                    throw new IndexOutOfBoundsException("Metadata is empty");
                });
            }

            // Build array of indices [0..n-1] and sort by UTF-8 name
            Integer[] order = new Integer[names.size()];
            for (int i = 0; i < names.size(); i++) {
                order[i] = i;
            }
            Arrays.sort(order, Comparator.comparing(names::get, Slice::compareTo));

            // builderIndex -> finalFieldId
            int[] builderIndexToFieldId = new int[names.size()];

            // Build final names in sorted order
            List<Slice> sortedNames = new ArrayList<>(names.size());
            for (int fieldId = 0; fieldId < names.size(); fieldId++) {
                int builderIndex = order[fieldId];
                sortedNames.add(names.get(builderIndex));
                builderIndexToFieldId[builderIndex] = fieldId;
            }

            // Mark builder as built
            nameToId = null;
            names = null;

            Metadata metadata = of(sortedNames);
            return new SortedMetadata(metadata, index -> builderIndexToFieldId[index]);
        }

        public record SortedMetadata(Metadata metadata, IntUnaryOperator sortedFieldIdMapping)
        {
            public SortedMetadata(Metadata metadata, IntUnaryOperator sortedFieldIdMapping)
            {
                this.metadata = requireNonNull(metadata, "metadata is null");
                this.sortedFieldIdMapping = requireNonNull(sortedFieldIdMapping, "sortedFieldIdMapping is null");
            }
        }

        private void checkNotBuilt()
        {
            checkState(nameToId != null && names != null, "Builder has already been built");
        }
    }
}
