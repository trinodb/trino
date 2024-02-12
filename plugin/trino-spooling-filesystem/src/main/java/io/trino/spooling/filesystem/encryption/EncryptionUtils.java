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
package io.trino.spooling.filesystem.encryption;

import com.google.crypto.tink.BinaryKeysetReader;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.StreamingAead;
import com.google.crypto.tink.streamingaead.StreamingAeadConfig;
import io.airlift.slice.Slice;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;

import static com.google.crypto.tink.BinaryKeysetWriter.withOutputStream;
import static io.airlift.slice.Slices.wrappedBuffer;

public class EncryptionUtils
{
    private static volatile boolean initialized;

    private EncryptionUtils() {}

    public static Slice generateRandomKey()
    {
        ensureInitialized();

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            KeysetHandle aes256KeySet = KeysetHandle.generateNew(KeyTemplates.get("AES256_GCM_HKDF_1MB"));
            CleartextKeysetHandle.write(aes256KeySet, withOutputStream(outputStream));
            return wrappedBuffer(outputStream.toByteArray());
        }
        catch (IOException | GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    private static KeysetHandle readKey(Slice key)
    {
        try {
            return CleartextKeysetHandle.read(BinaryKeysetReader.withBytes(key.getBytes()));
        }
        catch (IOException | GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public static OutputStream encryptingOutputStream(OutputStream output, Slice key)
            throws IOException
    {
        ensureInitialized();

        try {
            return readKey(key)
                    .getPrimitive(StreamingAead.class)
                    .newEncryptingStream(output, new byte[0]);
        }
        catch (GeneralSecurityException e) {
            throw new IOException("Could not initialize encryption", e);
        }
    }

    public static InputStream decryptingInputStream(InputStream input, Slice key)
            throws IOException
    {
        ensureInitialized();

        try {
            return readKey(key)
                    .getPrimitive(StreamingAead.class)
                    .newDecryptingStream(input, new byte[0]);
        }
        catch (GeneralSecurityException e) {
            throw new IOException("Could not initialize decryption", e);
        }
    }

    private static void ensureInitialized()
    {
        if (initialized) {
            return;
        }

        synchronized (EncryptionUtils.class) {
            if (initialized) {
                return;
            }

            try {
                StreamingAeadConfig.register();
                initialized = true;
            }
            catch (GeneralSecurityException e) {
                throw new RuntimeException("Could not initialize encryption", e);
            }
        }
    }
}
