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
package io.trino.execution.buffer;

import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.slice.Slice;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spiller.AesSpillCipher;
import io.trino.spiller.SpillCipher;

import javax.crypto.spec.SecretKeySpec;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class PagesSerdeFactory
{
    private final BlockEncodingSerde blockEncodingSerde;
    private final boolean compressionEnabled;

    public PagesSerdeFactory(BlockEncodingSerde blockEncodingSerde, boolean compressionEnabled)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.compressionEnabled = compressionEnabled;
    }

    public PagesSerde createPagesSerde(Optional<Slice> encryptionKey)
    {
        return createPagesSerdeInternal(encryptionKey.map(key -> {
            verify(key.hasByteArray(), "key is expected to be based on a byte array");
            return new AesSpillCipher(new SecretKeySpec(key.byteArray(), key.byteArrayOffset(), key.length(), "AES"));
        }));
    }

    public PagesSerde createPagesSerdeForSpill(Optional<SpillCipher> spillCipher)
    {
        return createPagesSerdeInternal(spillCipher);
    }

    private PagesSerde createPagesSerdeInternal(Optional<SpillCipher> spillCipher)
    {
        if (compressionEnabled) {
            return new PagesSerde(blockEncodingSerde, Optional.of(new Lz4Compressor()), Optional.of(new Lz4Decompressor()), spillCipher);
        }

        return new PagesSerde(blockEncodingSerde, Optional.empty(), Optional.empty(), spillCipher);
    }
}
