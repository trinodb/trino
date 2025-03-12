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
package io.trino.client.spooling.encoding;

import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.zstd.ZstdDecompressor;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Optional;

public class DecompressionUtils
{
    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
    private static final Optional<MethodHandle> LZ4_DECOMPRESSOR_CONSTRUCTOR =
            getDecompressorConstructor("io.airlift.compress.v3.lz4.Lz4NativeDecompressor");
    private static final Optional<MethodHandle> ZSTD_DECOMPRESSOR_CONSTRUCTOR =
            getDecompressorConstructor("io.airlift.compress.v3.zstd.ZstdNativeDecompressor");

    private DecompressionUtils() {}

    public static int decompressLZ4(byte[] input, byte[] output)
    {
        if (LZ4_DECOMPRESSOR_CONSTRUCTOR.isEmpty()) {
            Lz4Decompressor decompressor = new Lz4Decompressor();
            return decompressor.decompress(input, 0, input.length, output, 0, output.length);
        }

        return decompressNative(LZ4_DECOMPRESSOR_CONSTRUCTOR.get(), input, output);
    }

    public static int decompressZstd(byte[] input, byte[] output)
    {
        if (ZSTD_DECOMPRESSOR_CONSTRUCTOR.isEmpty()) {
            ZstdDecompressor decompressor = new ZstdDecompressor();
            return decompressor.decompress(input, 0, input.length, output, 0, output.length);
        }

        return decompressNative(ZSTD_DECOMPRESSOR_CONSTRUCTOR.get(), input, output);
    }

    private static int decompressNative(MethodHandle constructor, byte[] input, byte[] output)
    {
        try {
            Object decompressor = constructor.invoke();
            // int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            MethodHandle decompress = LOOKUP.findVirtual(decompressor.getClass(), "decompress", MethodType.methodType(int.class,
                    byte[].class, int.class, int.class, byte[].class, int.class, int.class));
            return (int) decompress.invoke(decompressor, input, 0, input.length, output, 0, output.length);
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static Optional<MethodHandle> getDecompressorConstructor(String clazzName)
    {
        try {
            Class<?> clazz = Class.forName(clazzName);
            MethodHandle constructor = LOOKUP.findConstructor(clazz, MethodType.methodType(void.class));
            MethodHandle isEnabled = LOOKUP.findStatic(clazz, "isEnabled", MethodType.methodType(boolean.class));
            if (!(boolean) isEnabled.invoke()) {
                // isEnabled return true only if the native library was loaded properly and all symbols are resolved
                return Optional.empty();
            }
            return Optional.of(constructor);
        }
        catch (Throwable e) {
            return Optional.empty();
        }
    }
}
