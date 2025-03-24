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
package org.apache.iceberg.variants;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

// Copied from Iceberg api/src/test/java/org/apache/iceberg/variants/VariantTestUtil.java
public class VariantTypeTestUtils
{
    private VariantTypeTestUtils() {}

    private static byte primitiveHeader(int primitiveType)
    {
        return (byte) (primitiveType << 2);
    }

    private static byte metadataHeader(boolean isSorted, int offsetSize)
    {
        return (byte) (((offsetSize - 1) << 6) | (isSorted ? 0b10000 : 0) | 0b0001);
    }

    /**
     * A hacky absolute put for ByteBuffer
     */
    private static int writeBufferAbsolute(ByteBuffer buffer, int offset, ByteBuffer toCopy)
    {
        int originalPosition = buffer.position();
        buffer.position(offset);
        ByteBuffer copy = toCopy.duplicate();
        buffer.put(copy); // duplicate so toCopy is not modified
        buffer.position(originalPosition);
        checkArgument(copy.remaining() <= 0, "Not fully written");
        return toCopy.remaining();
    }

    /**
     * Creates a random string primitive of the given length for forcing large offset sizes
     */
    static SerializedPrimitive createString(String string)
    {
        byte[] utf8 = string.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(5 + utf8.length).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put(0, primitiveHeader(16));
        buffer.putInt(1, utf8.length);
        writeBufferAbsolute(buffer, 5, ByteBuffer.wrap(utf8));
        return SerializedPrimitive.from(buffer, buffer.get(0));
    }

    public static ByteBuffer variantBuffer(Map<String, VariantValue> data)
    {
        ByteBuffer meta = createMetadata(data.keySet(), true /* sort names */);
        ByteBuffer value = createObject(meta, data);
        ByteBuffer buffer =
                ByteBuffer.allocate(meta.remaining() + value.remaining()).order(ByteOrder.LITTLE_ENDIAN);
        writeBufferAbsolute(buffer, 0, meta);
        writeBufferAbsolute(buffer, meta.remaining(), value);
        return buffer;
    }

    public static Variant variant(Map<String, VariantValue> data)
    {
        return Variant.from(variantBuffer(data));
    }

    public static ByteBuffer emptyMetadata()
    {
        return createMetadata(ImmutableList.of(), true);
    }

    public static ByteBuffer createMetadata(Collection<String> fieldNames, boolean sortNames)
    {
        if (fieldNames.isEmpty()) {
            return SerializedMetadata.EMPTY_V1_BUFFER;
        }

        int numElements = fieldNames.size();
        Stream<String> names = sortNames ? fieldNames.stream().sorted() : fieldNames.stream();
        ByteBuffer[] nameBuffers =
                names
                        .map(str -> ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8)))
                        .toArray(ByteBuffer[]::new);

        int dataSize = 0;
        for (ByteBuffer nameBuffer : nameBuffers) {
            dataSize += nameBuffer.remaining();
        }

        int offsetSize = VariantUtil.sizeOf(dataSize);
        int offsetListOffset = 1 /* header size */ + offsetSize /* dictionary size */;
        int dataOffset = offsetListOffset + ((1 + numElements) * offsetSize);
        int totalSize = dataOffset + dataSize;

        byte header = metadataHeader(sortNames, offsetSize);
        ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);

        buffer.put(0, header);
        VariantUtil.writeLittleEndianUnsigned(buffer, numElements, 1, offsetSize);

        // write offsets and strings
        int nextOffset = 0;
        int index = 0;
        for (ByteBuffer nameBuffer : nameBuffers) {
            // write the offset and the string
            VariantUtil.writeLittleEndianUnsigned(
                    buffer, nextOffset, offsetListOffset + (index * offsetSize), offsetSize);
            int nameSize = writeBufferAbsolute(buffer, dataOffset + nextOffset, nameBuffer);
            // update the offset and index
            nextOffset += nameSize;
            index += 1;
        }

        // write the final size of the data section
        VariantUtil.writeLittleEndianUnsigned(
                buffer, nextOffset, offsetListOffset + (index * offsetSize), offsetSize);

        return buffer;
    }

    public static ByteBuffer createObject(ByteBuffer metadataBuffer, Map<String, VariantValue> data)
    {
        // create the metadata to look up field names
        return createObject(SerializedMetadata.from(metadataBuffer), data);
    }

    public static ByteBuffer createObject(VariantMetadata metadata, Map<String, VariantValue> data)
    {
        int numElements = data.size();
        boolean isLarge = numElements > 0xFF;

        int dataSize = 0;
        for (Map.Entry<String, VariantValue> field : data.entrySet()) {
            dataSize += field.getValue().sizeInBytes();
        }

        // field ID size is the size needed to store the largest field ID in the data
        int fieldIdSize = VariantUtil.sizeOf(metadata.dictionarySize());
        int fieldIdListOffset = 1 /* header size */ + (isLarge ? 4 : 1) /* num elements size */;

        // offset size is the size needed to store the length of the data section
        int offsetSize = VariantUtil.sizeOf(dataSize);
        int offsetListOffset = fieldIdListOffset + (numElements * fieldIdSize);
        int dataOffset = offsetListOffset + ((1 + numElements) * offsetSize);
        int totalSize = dataOffset + dataSize;

        byte header = VariantUtil.objectHeader(isLarge, fieldIdSize, offsetSize);
        ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);

        buffer.put(0, header);
        if (isLarge) {
            buffer.putInt(1, numElements);
        }
        else {
            buffer.put(1, (byte) (numElements & 0xFF));
        }

        // write field IDs, values, and offsets
        int nextOffset = 0;
        int index = 0;
        List<String> sortedFieldNames = data.keySet().stream().sorted().collect(Collectors.toList());
        for (String fieldName : sortedFieldNames) {
            int id = metadata.id(fieldName);
            VariantUtil.writeLittleEndianUnsigned(
                    buffer, id, fieldIdListOffset + (index * fieldIdSize), fieldIdSize);
            VariantUtil.writeLittleEndianUnsigned(
                    buffer, nextOffset, offsetListOffset + (index * offsetSize), offsetSize);
            int valueSize = data.get(fieldName).writeTo(buffer, dataOffset + nextOffset);

            // update next offset and index
            nextOffset += valueSize;
            index += 1;
        }

        // write the final size of the data section
        VariantUtil.writeLittleEndianUnsigned(
                buffer, nextOffset, offsetListOffset + (index * offsetSize), offsetSize);

        return buffer;
    }

    static ByteBuffer createArray(Serialized... values)
    {
        int numElements = values.length;
        boolean isLarge = numElements > 0xFF;

        int dataSize = 0;
        for (Serialized value : values) {
            // TODO: produce size for every variant without serializing
            dataSize += value.buffer().remaining();
        }

        // offset size is the size needed to store the length of the data section
        int offsetSize = VariantUtil.sizeOf(dataSize);
        int offsetListOffset = 1 /* header size */ + (isLarge ? 4 : 1) /* num elements size */;
        int dataOffset = offsetListOffset + ((1 + numElements) * offsetSize) /* offset list size */;
        int totalSize = dataOffset + dataSize;

        byte header = VariantUtil.arrayHeader(isLarge, offsetSize);
        ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);

        buffer.put(0, header);
        if (isLarge) {
            buffer.putInt(1, numElements);
        }
        else {
            buffer.put(1, (byte) (numElements & 0xFF));
        }

        // write values and offsets
        int nextOffset = 0; // the first offset is always 0
        int index = 0;
        for (Serialized value : values) {
            // write the offset and value
            VariantUtil.writeLittleEndianUnsigned(
                    buffer, nextOffset, offsetListOffset + (index * offsetSize), offsetSize);
            // in a real implementation, the buffer should be passed to serialize
            ByteBuffer valueBuffer = value.buffer();
            int valueSize = writeBufferAbsolute(buffer, dataOffset + nextOffset, valueBuffer);
            // update next offset and index
            nextOffset += valueSize;
            index += 1;
        }

        // write the final size of the data section
        VariantUtil.writeLittleEndianUnsigned(
                buffer, nextOffset, offsetListOffset + (index * offsetSize), offsetSize);

        return buffer;
    }
}
