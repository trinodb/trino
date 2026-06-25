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
package io.trino.metadata;

import com.google.inject.Inject;
import io.trino.simd.BlockEncodingSimdSupport;
import io.trino.simd.BlockEncodingSimdSupport.SimdSupport;
import io.trino.spi.block.ArrayBlockEncoding;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.ByteArrayBlockEncoding;
import io.trino.spi.block.DictionaryBlockEncoding;
import io.trino.spi.block.Fixed12BlockEncoding;
import io.trino.spi.block.Int128ArrayBlockEncoding;
import io.trino.spi.block.IntArrayBlockEncoding;
import io.trino.spi.block.LongArrayBlockEncoding;
import io.trino.spi.block.MapBlockEncoding;
import io.trino.spi.block.RowBlockEncoding;
import io.trino.spi.block.RunLengthBlockEncoding;
import io.trino.spi.block.ShortArrayBlockEncoding;
import io.trino.spi.block.VariableWidthBlockEncoding;
import io.trino.spi.block.VariantBlockEncoding;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.simd.SimdCapability.COMPRESS_BYTE;
import static io.trino.simd.SimdCapability.COMPRESS_INT;
import static io.trino.simd.SimdCapability.COMPRESS_LONG;
import static io.trino.simd.SimdCapability.COMPRESS_SHORT;
import static io.trino.simd.SimdCapability.EXPAND_BYTE;
import static io.trino.simd.SimdCapability.EXPAND_INT;
import static io.trino.simd.SimdCapability.EXPAND_LONG;
import static io.trino.simd.SimdCapability.EXPAND_SHORT;
import static io.trino.simd.SimdCapability.NULL_BIT_PACKING;
import static java.util.Objects.requireNonNull;

public final class BlockEncodingManager
{
    // for deserialization
    private final Map<String, BlockEncoding> blockEncodingsByName = new ConcurrentHashMap<>();
    // for serialization
    private final Map<Class<? extends Block>, BlockEncoding> blockEncodingNamesByClass = new ConcurrentHashMap<>();

    @Inject
    public BlockEncodingManager(BlockEncodingSimdSupport blockEncodingSimdSupport)
    {
        // add the built-in BlockEncodings
        SimdSupport simdSupport = blockEncodingSimdSupport.getSimdSupport();
        addBlockEncoding(new VariableWidthBlockEncoding(simdSupport.supports(NULL_BIT_PACKING)));
        addBlockEncoding(new ByteArrayBlockEncoding(simdSupport.supports(NULL_BIT_PACKING), simdSupport.supports(COMPRESS_BYTE), simdSupport.supports(EXPAND_BYTE)));
        addBlockEncoding(new ShortArrayBlockEncoding(simdSupport.supports(NULL_BIT_PACKING), simdSupport.supports(COMPRESS_SHORT), simdSupport.supports(EXPAND_SHORT)));
        addBlockEncoding(new IntArrayBlockEncoding(simdSupport.supports(NULL_BIT_PACKING), simdSupport.supports(COMPRESS_INT), simdSupport.supports(EXPAND_INT)));
        addBlockEncoding(new LongArrayBlockEncoding(simdSupport.supports(NULL_BIT_PACKING), simdSupport.supports(COMPRESS_LONG), simdSupport.supports(EXPAND_LONG)));
        addBlockEncoding(new Fixed12BlockEncoding(simdSupport.supports(NULL_BIT_PACKING)));
        addBlockEncoding(new Int128ArrayBlockEncoding(simdSupport.supports(NULL_BIT_PACKING)));
        addBlockEncoding(new VariantBlockEncoding(simdSupport.supports(NULL_BIT_PACKING)));
        addBlockEncoding(new DictionaryBlockEncoding());
        addBlockEncoding(new ArrayBlockEncoding(simdSupport.supports(NULL_BIT_PACKING)));
        addBlockEncoding(new MapBlockEncoding(simdSupport.supports(NULL_BIT_PACKING)));
        addBlockEncoding(new RowBlockEncoding(simdSupport.supports(NULL_BIT_PACKING)));
        addBlockEncoding(new RunLengthBlockEncoding());
    }

    public BlockEncoding getBlockEncodingByName(String encodingName)
    {
        BlockEncoding blockEncoding = blockEncodingsByName.get(encodingName);
        checkArgument(blockEncoding != null, "Unknown block encoding: %s", encodingName);
        return blockEncoding;
    }

    public BlockEncoding getBlockEncodingByBlockClass(Class<? extends Block> clazz)
    {
        BlockEncoding blockEncoding = blockEncodingNamesByClass.get(clazz);
        checkArgument(blockEncoding != null, "Unknown block encoding for block: %s", clazz.getName());
        return blockEncoding;
    }

    public void addBlockEncoding(BlockEncoding blockEncoding)
    {
        requireNonNull(blockEncoding, "blockEncoding is null");
        BlockEncoding existingEntryByClass = blockEncodingNamesByClass.putIfAbsent(blockEncoding.getBlockClass(), blockEncoding);
        checkArgument(existingEntryByClass == null, "Encoding already registered: %s", blockEncoding.getName());
        BlockEncoding existingEntryByName = blockEncodingsByName.putIfAbsent(blockEncoding.getName(), blockEncoding);
        checkArgument(existingEntryByName == null, "Encoding already registered: %s", blockEncoding.getName());
    }
}
