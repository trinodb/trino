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
import java.util.Optional;

import static java.lang.invoke.MethodType.methodType;

public class DecompressionUtils
{
    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
    private static final Optional<NativeDecompressor> LZ4_NATIVE_DECOMPRESSOR =
            createNativeDecompressor("io.airlift.compress.v3.lz4.Lz4NativeDecompressor");
    private static final Optional<NativeDecompressor> ZSTD_NATIVE_DECOMPRESSOR =
            createNativeDecompressor("io.airlift.compress.v3.zstd.ZstdNativeDecompressor");

    private DecompressionUtils() {}

    public static int decompressLZ4(byte[] input, byte[] output)
    {
        if (LZ4_NATIVE_DECOMPRESSOR.isEmpty()) {
            Lz4Decompressor decompressor = new Lz4Decompressor();
            return decompressor.decompress(input, 0, input.length, output, 0, output.length);
        }

        return LZ4_NATIVE_DECOMPRESSOR.get().decompress(input, output);
    }

    public static int decompressZstd(byte[] input, byte[] output)
    {
        if (ZSTD_NATIVE_DECOMPRESSOR.isEmpty()) {
            ZstdDecompressor decompressor = new ZstdDecompressor();
            return decompressor.decompress(input, 0, input.length, output, 0, output.length);
        }

        return ZSTD_NATIVE_DECOMPRESSOR.get().decompress(input, output);
    }

    private static Optional<NativeDecompressor> createNativeDecompressor(String clazzName)
    {
        try {
            Class<?> clazz = Class.forName(clazzName);
            MethodHandle isEnabled = LOOKUP.findStatic(clazz, "isEnabled", methodType(boolean.class));
            if (!(boolean) isEnabled.invoke()) {
                // isEnabled return true only if the native library was loaded properly and all symbols are resolved
                return Optional.empty();
            }
            MethodHandle constructor = LOOKUP.findConstructor(clazz, methodType(void.class));
            // int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            MethodHandle decompress = LOOKUP.findVirtual(clazz, "decompress", methodType(
                    int.class, byte[].class, int.class, int.class, byte[].class, int.class, int.class));
            return Optional.of(new NativeDecompressor(constructor, decompress));
        }
        catch (Throwable e) {
            return Optional.empty();
        }
    }

    // Holds the method handles resolved once at class initialization to avoid a virtual lookup on every segment decode.
    private static final class NativeDecompressor
    {
        private final MethodHandle constructor;
        private final MethodHandle decompress;

        private NativeDecompressor(MethodHandle constructor, MethodHandle decompress)
        {
            this.constructor = constructor;
            this.decompress = decompress;
        }

        public int decompress(byte[] input, byte[] output)
        {
            try {
                // The native decompressors are not guaranteed to be thread-safe, so a fresh instance is created per call
                Object decompressor = constructor.invoke();
                return (int) decompress.invoke(decompressor, input, 0, input.length, output, 0, output.length);
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
    }
}
