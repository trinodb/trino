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

package io.trino.spi.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static io.trino.spi.block.EncoderUtil.decodeNullBitsScalar;
import static io.trino.spi.block.EncoderUtil.decodeNullBitsVectorized;
import static io.trino.spi.block.EncoderUtil.encodeNullsAsBitsScalar;
import static io.trino.spi.block.EncoderUtil.encodeNullsAsBitsVectorized;

public class VariantBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "VARIANT";

    private final boolean vectorizeNullBitPacking;

    public VariantBlockEncoding(boolean vectorizeNullBitPacking)
    {
        this.vectorizeNullBitPacking = vectorizeNullBitPacking;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Class<? extends Block> getBlockClass()
    {
        return VariantBlock.class;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        VariantBlock variantBlock = (VariantBlock) block;

        sliceOutput.appendInt(variantBlock.getPositionCount());

        blockEncodingSerde.writeBlock(sliceOutput, variantBlock.getMetadata());
        blockEncodingSerde.writeBlock(sliceOutput, variantBlock.getValues());

        if (vectorizeNullBitPacking) {
            encodeNullsAsBitsVectorized(sliceOutput, variantBlock.getRawIsNull(), variantBlock.getOffsetBase(), variantBlock.getPositionCount());
        }
        else {
            encodeNullsAsBitsScalar(sliceOutput, variantBlock.getRawIsNull(), variantBlock.getOffsetBase(), variantBlock.getPositionCount());
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        Block metadataBlock = blockEncodingSerde.readBlock(sliceInput);
        Block valuesBlock = blockEncodingSerde.readBlock(sliceInput);

        boolean[] variantIsNull;
        if (vectorizeNullBitPacking) {
            variantIsNull = decodeNullBitsVectorized(sliceInput, positionCount).orElse(null);
        }
        else {
            variantIsNull = decodeNullBitsScalar(sliceInput, positionCount).orElse(null);
        }

        return VariantBlock.createInternal(0, positionCount, variantIsNull, metadataBlock, valuesBlock);
    }
}
