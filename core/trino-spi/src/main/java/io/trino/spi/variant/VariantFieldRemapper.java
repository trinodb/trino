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

import java.util.Arrays;
import java.util.function.IntUnaryOperator;

import static io.trino.spi.variant.VariantDecoder.decode;
import static io.trino.spi.variant.VariantUtils.getOffsetSize;
import static io.trino.spi.variant.VariantUtils.verify;
import static java.lang.Math.max;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

/// Remaps field IDs in a Variant for a new metadata dictionary.
/// Remapping is necessary when merging Variants with different metadata dictionaries into
/// a single Variant (e.g., array or object) which requires a unified metadata dictionary.
/// Remapping is a complex operation because field IDs are encoded depending on the maximum field ID
/// and object sizes are encoded depending on the total size of the object. Thus, when
/// multiple Variants are merged, the field IDs and object sizes may change, and the entire Variant
/// must be rewritten with the new encodings.
///
/// Remapping is done in two phases:
/// 1. Initial creation of the remapper with provisional field IDs assigned to the fields
///    using the `Metadata.Builder` to track which fields are used.
/// 2. Finalization of the remapper where provisional field IDs are updated to final field IDs
///    after the globally sorted metadata dictionary is created. After finalization, the
///    final size of the remapped Variant can be fetched with the `size()` method.
/// 3. Writing the remapped Variant to an output slice with the `write()` method.
public final class VariantFieldRemapper
{
    private enum RemapMode
    {
        NONE,
        IDENTITY,
        SAME_SIZE,
        RESIZE,
    }

    private final int[] fieldIdMapping;
    private final int originalFieldIdEncodedWidth;

    private final Slice variant;

    private RemapMode remapMode;
    private int size = -1;

    private Int2IntOpenHashMap containerSizeCache;

    public static VariantFieldRemapper create(Variant variant, Metadata.Builder metadataBuilder)
    {
        requireNonNull(variant, "variant is null");
        requireNonNull(metadataBuilder, "metadataBuilder is null");

        // Fast path: no fields, so there is nothing to merge/remap.
        if (variant.metadata().dictionarySize() == 0) {
            return new VariantFieldRemapper(variant.data());
        }
        return new Builder(variant, metadataBuilder).build();
    }

    private VariantFieldRemapper(Slice variant)
    {
        this.variant = requireNonNull(variant, "variant is null");

        this.fieldIdMapping = new int[0];
        this.originalFieldIdEncodedWidth = -1;
        this.remapMode = RemapMode.NONE;
        this.size = variant.length();
    }

    private VariantFieldRemapper(Slice variant, int[] fieldIdMapping, int originalFieldIdEncodedWidth)
    {
        this.fieldIdMapping = requireNonNull(fieldIdMapping, "fieldIdMapping is null");
        this.originalFieldIdEncodedWidth = originalFieldIdEncodedWidth;
        this.variant = requireNonNull(variant, "variant is null");
    }

    /// Finalizes the field remapping value by updating the provisional field IDs to final field IDs.
    /// The system creates a globally sorted metadata dictionary after all values have been planned,
    /// which may change the field IDs assigned during initial setup.
    /// This method must be called before `size()` or `remapVariant()`.
    // Note: This method can rely on the field IDs being assigned in ascending order for determining the write order of object fields.
    public void finalize(IntUnaryOperator remapFieldIds)
    {
        if (remapMode == RemapMode.NONE) {
            return;
        }
        if (remapMode != null) {
            throw new IllegalStateException("finalize() already called");
        }

        boolean identity = true;
        int maxFieldId = -1;
        for (int variantFieldId = 0; variantFieldId < fieldIdMapping.length; variantFieldId++) {
            int provisionalFieldId = fieldIdMapping[variantFieldId];
            if (provisionalFieldId >= 0) {
                int finalFieldId = remapFieldIds.applyAsInt(provisionalFieldId);
                fieldIdMapping[variantFieldId] = finalFieldId;
                identity &= (finalFieldId == variantFieldId);
                maxFieldId = max(maxFieldId, finalFieldId);
            }
        }
        if (identity) {
            remapMode = RemapMode.IDENTITY;
        }
        else if (originalFieldIdEncodedWidth == getOffsetSize(maxFieldId)) {
            // fast path where original and final field id encoded widths are both 1 byte
            // this cannot be used if size is > 1, because an object inside the
            // variant could have originally been encoded with a larger field offset size, and
            // with the compacted metadata dictionary, the max field id could be smaller, allowing
            // the field offset size to be reduced.
            remapMode = RemapMode.SAME_SIZE;
        }
        else {
            remapMode = RemapMode.RESIZE;
        }

        // compute size of remapped variant data
        if (remapMode == RemapMode.IDENTITY) {
            size = variant.length();
        }
        else {
            containerSizeCache = new Int2IntOpenHashMap(16);
            size = calculateFullyRemappedSize(variant, 0, variant.length(), containerSizeCache);
        }
    }

    /// Returns the size, in bytes, required to write the variant.
    /// This cannot be called before `finalize()`.
    public int size()
    {
        if (remapMode == null) {
            throw new IllegalStateException("size() called before finalize()");
        }
        return size;
    }

    /// Writes the value to the given output slice at the specified offset.
    /// This must be called after `finalize()`.
    /// This method can be called multiple times to write the same value to different output slices.
    ///
    /// @return the number of bytes written, which is equal to `size()`
    public int write(Slice output, int outputOffset)
    {
        if (remapMode == null) {
            throw new IllegalStateException("remapVariant() called before finalize()");
        }
        if (checkFromIndexSize(outputOffset, size, output.length()) < 0) {
            throw new IllegalArgumentException("Output slice is too small to remap variant");
        }

        if (remapMode == RemapMode.IDENTITY || remapMode == RemapMode.NONE) {
            output.setBytes(outputOffset, variant);
            return size;
        }

        int written = write(variant, 0, output, outputOffset, variant.length());
        verify(written == size, "unexpected size mismatch in remapVariant");
        return written;
    }

    private int calculateFullyRemappedSize(Slice data, int offset, int length, Int2IntOpenHashMap containerSizeCache)
    {
        return switch (decode(data, offset)) {
            case VariantDecoder.ArrayLayout array -> {
                int totalChildrenLength = 0;
                for (int index = 0; index < array.count(); index++) {
                    int start = array.elementStart(index);
                    int end = array.elementEnd(index);
                    totalChildrenLength += calculateFullyRemappedSize(data, start, end - start, containerSizeCache);
                }
                int arrayTotalSize = VariantEncoder.encodedArraySize(array.count(), totalChildrenLength);
                containerSizeCache.putIfAbsent(offset, arrayTotalSize);
                yield arrayTotalSize;
            }
            case VariantDecoder.ObjectLayout object -> {
                int maxFieldId = -1;
                int totalChildrenLength = 0;

                for (int index = 0; index < object.count(); index++) {
                    int remappedFieldId = fieldIdMapping[object.fieldId(index)];
                    maxFieldId = max(maxFieldId, remappedFieldId);

                    int start = object.valueStart(index);
                    int end = object.valueEnd(index);
                    totalChildrenLength += calculateFullyRemappedSize(data, start, end - start, containerSizeCache);
                }

                int objectTotalSize = VariantEncoder.encodedObjectSize(maxFieldId, object.count(), totalChildrenLength);
                containerSizeCache.putIfAbsent(offset, objectTotalSize);
                yield objectTotalSize;
            }
            case VariantDecoder.PrimitiveLayout _ -> length;
        };
    }

    private int write(Slice input, int inputOffset, Slice output, int outputOffset, int length)
    {
        return switch (decode(input, inputOffset)) {
            case VariantDecoder.ArrayLayout array -> {
                // write array header
                int written = switch (remapMode) {
                    case SAME_SIZE -> {
                        // Header is unchanged byte-for-byte when SAME_SIZE.
                        int headerSize = array.headerSize();
                        output.setBytes(outputOffset, input, inputOffset, headerSize);
                        yield headerSize;
                    }
                    case RESIZE -> VariantEncoder.encodeArrayHeading(
                            array.count(),
                            i -> {
                                int start = array.elementStart(i);

                                int cachedContainerSize = containerSizeCache.get(start);
                                if (cachedContainerSize != Int2IntOpenHashMap.DEFAULT_RETURN_VALUE) {
                                    return cachedContainerSize;
                                }
                                return array.elementEnd(i) - start;
                            },
                            output,
                            outputOffset);
                    case IDENTITY, NONE -> throw new IllegalStateException("unexpected remap mode " + remapMode);
                };

                // write remapped elements
                for (int index = 0; index < array.count(); index++) {
                    int start = array.elementStart(index);
                    int end = array.elementEnd(index);
                    written += write(input, start, output, outputOffset + written, end - start);
                }

                verify(written == (remapMode == RemapMode.SAME_SIZE ? length : containerSizeCache.get(inputOffset)), "unexpected size mismatch in complete remap");
                yield written;
            }
            case VariantDecoder.ObjectLayout object -> {
                // write the object header with remapped field IDs
                int written = VariantEncoder.encodeObjectHeading(
                        object.count(),
                        i -> fieldIdMapping[object.fieldId(i)],
                        i -> {
                            int start = object.valueStart(i);
                            if (remapMode == RemapMode.RESIZE) {
                                int cachedSize = containerSizeCache.get(start);
                                if (cachedSize != Int2IntOpenHashMap.DEFAULT_RETURN_VALUE) {
                                    return cachedSize;
                                }
                            }
                            return object.valueEnd(i) - start;
                        },
                        output,
                        outputOffset);

                // write remapped elements
                for (int index = 0; index < object.count(); index++) {
                    int start = object.valueStart(index);
                    int end = object.valueEnd(index);
                    written += write(input, start, output, outputOffset + written, end - start);
                }

                verify(written == (remapMode == RemapMode.SAME_SIZE ? length : containerSizeCache.get(inputOffset)), "unexpected size mismatch in complete remap");
                yield written;
            }
            case VariantDecoder.PrimitiveLayout _ -> {
                output.setBytes(outputOffset, input, inputOffset, length);
                yield length;
            }
        };
    }

    private static final class Builder
    {
        private final Variant variant;
        private final Metadata.Builder metadataBuilder;
        private int[] variantFieldIdToProvisionalFieldId;
        private int maxEnabledFieldId = -1;
        private int enabledFieldCount;

        private Builder(Variant variant, Metadata.Builder metadataBuilder)
        {
            this.variant = requireNonNull(variant, "variant is null");
            this.metadataBuilder = requireNonNull(metadataBuilder, "metadataBuilder is null");

            if (variant.metadata().dictionarySize() == 0) {
                variantFieldIdToProvisionalFieldId = new int[0];
                return;
            }

            mapVariantFields(variant.data(), 0);
        }

        private boolean isFullyMapped()
        {
            return enabledFieldCount == variant.metadata().dictionarySize();
        }

        public void enableField(int variantFieldId)
        {
            if (variantFieldIdToProvisionalFieldId == null) {
                variantFieldIdToProvisionalFieldId = new int[variant.metadata().dictionarySize()];
                Arrays.fill(variantFieldIdToProvisionalFieldId, -1);
            }
            if (variantFieldIdToProvisionalFieldId[variantFieldId] == -1) {
                variantFieldIdToProvisionalFieldId[variantFieldId] = metadataBuilder.addFieldName(variant.metadata().get(variantFieldId));
                if (variantFieldId > maxEnabledFieldId) {
                    maxEnabledFieldId = variantFieldId;
                }
                enabledFieldCount++;
            }
        }

        private void mapVariantFields(Slice data, int offset)
        {
            switch (decode(data, offset)) {
                case VariantDecoder.ArrayLayout array -> {
                    for (int index = 0; index < array.count(); index++) {
                        mapVariantFields(data, array.elementStart(index));
                        if (isFullyMapped()) {
                            return;
                        }
                    }
                }
                case VariantDecoder.ObjectLayout object -> {
                    for (int index = 0; index < object.count(); index++) {
                        enableField(object.fieldId(index));
                        if (isFullyMapped()) {
                            return;
                        }
                        mapVariantFields(data, object.valueStart(index));
                        if (isFullyMapped()) {
                            return;
                        }
                    }
                }
                case VariantDecoder.PrimitiveLayout _ -> {
                    // No fields to map
                }
            }
        }

        public VariantFieldRemapper build()
        {
            if (variantFieldIdToProvisionalFieldId == null) {
                return new VariantFieldRemapper(variant.data());
            }
            return new VariantFieldRemapper(variant.data(), variantFieldIdToProvisionalFieldId, getOffsetSize(maxEnabledFieldId));
        }
    }
}
