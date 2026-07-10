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
package io.trino.spi.statestore;

import java.io.Serializable;

/**
 * CipherService used for encryption and decryption
 *
 * @param <T> value type to encrypt
 * @since 2020-03-20
 */
public interface CipherService<T extends Serializable>
{
    /**
     * Types of encryption algorithms
     *
     * @since 2020-03-20
     */
    enum Type
    {
        NONE, BASE64
    }

    /**
     * Encrypt given object into String
     *
     * @param object object to be encrypted
     * @return encrypted String
     */
    String encrypt(T object);

    /**
     * Decrypt given encrypted String
     *
     * @param encryptedValue encrypted String
     * @return decrypted object
     */
    T decrypt(String encryptedValue);
}
