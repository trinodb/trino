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

import io.trino.Session;
import io.trino.spi.block.BlockEncodingSerde;

import static io.trino.SystemSessionProperties.getExchangeCompressionCodec;

public final class PagesSerdes
{
    private PagesSerdes() {}

    public static PagesSerdeFactory createExchangePagesSerdeFactory(BlockEncodingSerde blockEncodingSerde, Session session)
    {
        return new PagesSerdeFactory(blockEncodingSerde, getExchangeCompressionCodec(session));
    }

    public static PagesSerdeFactory createSpillingPagesSerdeFactory(BlockEncodingSerde blockEncodingSerde, CompressionCodec compressionCodec)
    {
        return new PagesSerdeFactory(blockEncodingSerde, compressionCodec);
    }
}
