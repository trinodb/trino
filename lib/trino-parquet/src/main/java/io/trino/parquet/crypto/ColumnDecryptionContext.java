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
package io.trino.parquet.crypto;

import org.apache.parquet.format.BlockCipher.Decryptor;

import java.util.Arrays;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public record ColumnDecryptionContext(Decryptor dataDecryptor, Decryptor metadataDecryptor, byte[] fileAad)
{
    public ColumnDecryptionContext(Decryptor dataDecryptor, Decryptor metadataDecryptor, byte[] fileAad)
    {
        this.dataDecryptor = requireNonNull(dataDecryptor, "dataDecryptor is null");
        this.metadataDecryptor = requireNonNull(metadataDecryptor, "metadataDecryptor is null");
        this.fileAad = requireNonNull(fileAad, "fileAad is null");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnDecryptionContext context)) {
            return false;
        }
        return Objects.equals(dataDecryptor, context.dataDecryptor)
                && Objects.equals(metadataDecryptor, context.metadataDecryptor)
                && Objects.deepEquals(fileAad, context.fileAad);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(dataDecryptor, metadataDecryptor, Arrays.hashCode(fileAad));
    }
}
