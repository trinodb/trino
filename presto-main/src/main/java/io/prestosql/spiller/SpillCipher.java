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
package io.prestosql.spiller;

public interface SpillCipher
        extends AutoCloseable
{
    /**
     * Calculates the maximum required buffer size to encrypt input data with the given length
     */
    int encryptedMaxLength(int inputLength);

    /**
     * Encrypts the contents of the input buffer into the destination buffer returning the number
     * of bytes written into the destination buffer
     *
     * @return The length of the encrypted content in the destination buffer
     */
    int encrypt(byte[] data, int inputOffset, int length, byte[] destination, int destinationOffset);

    /**
     * Calculates the required buffer size to decrypt data with the given encrypted length
     */
    int decryptedMaxLength(int encryptedLength);

    /**
     * Decrypts the contents of the input buffer into the destination buffer, returning the number of bytes
     * written into the destination buffer
     *
     * @return The length of the decrypted content in destination buffer
     */
    int decrypt(byte[] encryptedData, int inputOffset, int length, byte[] destination, int destinationOffset);

    /**
     * Destroys the cipher, preventing future use. Implementations should allow this to be called multiple times
     * without failing.
     */
    @Override
    void close();
}
