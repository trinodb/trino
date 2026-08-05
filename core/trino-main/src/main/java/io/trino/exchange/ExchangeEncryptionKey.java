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
package io.trino.exchange;

import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.execution.buffer.OutputBuffer;

import java.util.Optional;

public final class ExchangeEncryptionKey
{
    private ExchangeEncryptionKey() {}

    public static Optional<Slice> keyFor(Session session, OutputBuffer outputBuffer)
    {
        return effectiveKey(session, outputBuffer.usesExternalStorage());
    }

    public static Optional<Slice> keyFor(Session session, ExchangeDataSource exchangeDataSource)
    {
        return effectiveKey(session, exchangeDataSource.usesExternalStorage());
    }

    public static Optional<Slice> keyForDirectExchange(Session session)
    {
        return effectiveKey(session, false);
    }

    private static Optional<Slice> effectiveKey(Session session, boolean encryptionRequired)
    {
        if (encryptionRequired) {
            return session.getExchangeEncryptionKey();
        }
        return Optional.empty();
    }
}
