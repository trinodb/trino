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
package io.trino.plugin.iceberg;

import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.assertj.core.api.Assertions.assertThat;

class TestIcebergAvroDataConversion
{
    @Test
    void testToIcebergAvroObjectWithDictionaryWrappedVariantBlock()
    {
        BlockBuilder blockBuilder = VARIANT.createBlockBuilder(null, 2);
        VARIANT.writeObject(blockBuilder, io.trino.spi.variant.Variant.ofLong(41));
        VARIANT.writeObject(blockBuilder, io.trino.spi.variant.Variant.ofLong(42));
        Block dictionary = DictionaryBlock.create(3, blockBuilder.build(), new int[] {1, 0, 1});

        Object converted = IcebergAvroDataConversion.toIcebergAvroObject(VARIANT, Types.VariantType.get(), dictionary, 0);

        assertThat(converted).isInstanceOf(org.apache.iceberg.variants.Variant.class);
        assertThat(org.apache.iceberg.variants.Variant.toString((org.apache.iceberg.variants.Variant) converted))
                .contains("type=INT64")
                .contains("value=42");
    }

    @Test
    void testToIcebergAvroObjectWithRunLengthEncodedVariantBlock()
    {
        BlockBuilder blockBuilder = VARIANT.createBlockBuilder(null, 1);
        VARIANT.writeObject(blockBuilder, io.trino.spi.variant.Variant.ofString("hello"));
        Block rle = RunLengthEncodedBlock.create(blockBuilder.build(), 3);

        Object converted = IcebergAvroDataConversion.toIcebergAvroObject(VARIANT, Types.VariantType.get(), rle, 1);

        assertThat(converted).isInstanceOf(org.apache.iceberg.variants.Variant.class);
        assertThat(org.apache.iceberg.variants.Variant.toString((org.apache.iceberg.variants.Variant) converted))
                .contains("type=STRING")
                .contains("value=hello");
    }

    @Test
    void testSerializeFixedBinaryToTrinoBlock()
    {
        BlockBuilder blockBuilder = VARBINARY.createBlockBuilder(null, 1);

        IcebergAvroDataConversion.serializeToTrinoBlock(VARBINARY, Types.FixedType.ofLength(3), blockBuilder, new byte[] {1, 2, 3});

        assertThat(VARBINARY.getSlice(blockBuilder.build(), 0)).isEqualTo(Slices.wrappedBuffer(new byte[] {1, 2, 3}));
    }

    @Test
    void testSerializeBinaryToTrinoBlock()
    {
        BlockBuilder blockBuilder = VARBINARY.createBlockBuilder(null, 1);

        IcebergAvroDataConversion.serializeToTrinoBlock(VARBINARY, Types.BinaryType.get(), blockBuilder, ByteBuffer.wrap(new byte[] {4, 5, 6}));

        assertThat(VARBINARY.getSlice(blockBuilder.build(), 0)).isEqualTo(Slices.wrappedBuffer(new byte[] {4, 5, 6}));
    }
}
