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

import io.airlift.slice.Slice;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.spi.Page;
import io.trino.spi.block.BlockEncodingSerde;

import javax.crypto.SecretKey;

import java.util.Optional;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;

public class TestingPagesSerdeFactory
        extends PagesSerdeFactory
{
    private static final InternalBlockEncodingSerde BLOCK_ENCODING_SERDE = new InternalBlockEncodingSerde(new BlockEncodingManager(), TESTING_TYPE_MANAGER);

    public TestingPagesSerdeFactory()
    {
        // compression should be enabled in as many tests as possible
        super(BLOCK_ENCODING_SERDE, true);
    }

    public static PagesSerde testingPagesSerde()
    {
        return new SynchronizedPagesSerde(
                BLOCK_ENCODING_SERDE,
                true,
                Optional.empty());
    }

    private static class SynchronizedPagesSerde
            extends PagesSerde
    {
        public SynchronizedPagesSerde(BlockEncodingSerde blockEncodingSerde, boolean compressionEnabled, Optional<SecretKey> encryptionKey)
        {
            super(blockEncodingSerde, compressionEnabled, encryptionKey);
        }

        @Override
        public synchronized Slice serialize(Page page)
        {
            return super.serialize(page);
        }

        @Override
        public synchronized Page deserialize(Slice page)
        {
            return super.deserialize(page);
        }
    }
}
