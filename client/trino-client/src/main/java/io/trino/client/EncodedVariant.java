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
package io.trino.client;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class EncodedVariant
{
    private final byte[] metadataBytes;
    private final byte[] valueBytes;

    private EncodedVariant(byte[] metadataBytes, byte[] valueBytes)
    {
        this.metadataBytes = requireNonNull(metadataBytes, "metadataBytes is null");
        this.valueBytes = requireNonNull(valueBytes, "valueBytes is null");
        checkArgument(valueBytes.length > 0, "valueBytes is empty");
    }

    public static EncodedVariant fromBytes(byte[] metadataBytes, byte[] valueBytes)
    {
        requireNonNull(metadataBytes, "metadataBytes is null");
        requireNonNull(valueBytes, "valueBytes is null");
        return new EncodedVariant(metadataBytes, valueBytes);
    }

    public byte[] getMetadataBytes()
    {
        return metadataBytes;
    }

    public byte[] getValueBytes()
    {
        return valueBytes;
    }
}
