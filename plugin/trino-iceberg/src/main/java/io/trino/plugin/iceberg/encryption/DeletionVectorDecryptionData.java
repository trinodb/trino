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
package io.trino.plugin.iceberg.encryption;

import io.airlift.slice.SizeOf;

import java.util.Arrays;
import java.util.Objects;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

/**
 * Decryption data for a Puffin deletion vector, which also needs the encrypted file length.
 */
public record DeletionVectorDecryptionData(byte[] fileEncryptionKey, byte[] fileAadPrefix, long fileLength)
        implements FileDecryptionData
{
    public static final String TYPE = "deletionVector";
    private static final int INSTANCE_SIZE = instanceSize(DeletionVectorDecryptionData.class);

    public DeletionVectorDecryptionData
    {
        requireNonNull(fileEncryptionKey, "fileEncryptionKey is null");
        requireNonNull(fileAadPrefix, "fileAadPrefix is null");
    }

    @Override
    public String type()
    {
        return TYPE;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + SizeOf.sizeOf(fileEncryptionKey) + SizeOf.sizeOf(fileAadPrefix);
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof DeletionVectorDecryptionData other
                && Arrays.equals(fileEncryptionKey, other.fileEncryptionKey)
                && Arrays.equals(fileAadPrefix, other.fileAadPrefix)
                && fileLength == other.fileLength;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(Arrays.hashCode(fileEncryptionKey), Arrays.hashCode(fileAadPrefix), fileLength);
    }

    @Override
    public String toString()
    {
        return "[REDACTED]";
    }
}
