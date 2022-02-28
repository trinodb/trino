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
package io.trino.util;

import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;
import org.apache.commons.pool2.DestroyMode;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.SoftReferenceObjectPool;

import java.nio.ByteBuffer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class ThreadSafeCompressorDecompressor
        implements Compressor, Decompressor
{
    private final SoftReferenceObjectPool<Compressor> compressors;
    private final SoftReferenceObjectPool<Decompressor> decompressors;

    public ThreadSafeCompressorDecompressor(Supplier<Compressor> compressorFactory, Supplier<Decompressor> decompressorFactory)
    {
        this.compressors = new SoftReferenceObjectPool<>(new CompressorObjectFactory<>(requireNonNull(compressorFactory, "compressorFactory is null")));
        this.decompressors = new SoftReferenceObjectPool<>(new CompressorObjectFactory<>(requireNonNull(decompressorFactory, "decompressorFactory is null")));
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return with(compressors, compressor -> compressor.maxCompressedLength(uncompressedSize));
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        return with(compressors, compressor -> compressor.compress(input, inputOffset, inputLength, output, outputOffset, maxOutputLength));
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        with(compressors, compressor -> {
            compressor.compress(input, output);
            return true;
        });
    }

    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength) throws MalformedInputException
    {
        return with(decompressors, decompressor -> decompressor.decompress(input, inputOffset, inputLength, output, outputOffset, maxOutputLength));
    }

    @Override
    public void decompress(ByteBuffer input, ByteBuffer output) throws MalformedInputException
    {
        with(decompressors, decompressor ->
        {
            decompressor.decompress(input, output);
            return true;
        });
    }

    private <T, U> T with(ObjectPool<U> pool, Function<U, T> call)
    {
        U delegate = null;
        try {
            delegate = pool.borrowObject();
            return call.apply(delegate);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            if (delegate != null) {
                try {
                    pool.returnObject(delegate);
                }
                catch (Exception ignored) {
                }
            }
        }
    }

    private static class CompressorObjectFactory<T>
            implements PooledObjectFactory<T>
    {
        private final Supplier<T> delegate;

        CompressorObjectFactory(Supplier<T> delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void activateObject(PooledObject<T> pooledObject)
        {
        }

        @Override
        public void destroyObject(PooledObject<T> pooledObject)
        {
        }

        @Override
        public void destroyObject(PooledObject<T> pooledObject, DestroyMode destroyMode)
        {
            destroyObject(pooledObject);
        }

        @Override
        public PooledObject<T> makeObject()
        {
            return new DefaultPooledObject<>(delegate.get());
        }

        @Override
        public void passivateObject(PooledObject<T> pooledObject)
        {
        }

        @Override
        public boolean validateObject(PooledObject<T> pooledObject)
        {
            return true;
        }
    }
}
