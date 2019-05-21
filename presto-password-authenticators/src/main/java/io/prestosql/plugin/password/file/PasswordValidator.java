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
package io.prestosql.plugin.password.file;

import com.google.common.cache.Cache;
import io.airlift.log.Logger;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static io.prestosql.plugin.password.file.EncryptionUtil.isBCryptPasswordValid;
import static io.prestosql.plugin.password.file.EncryptionUtil.isPBKDF2PasswordValid;

class PasswordValidator
{
    private static final Logger log = Logger.get(PasswordValidator.class);

    private PasswordValidator() {}

    public static boolean authenticate(String inputUsername, String inputPassword, String storedUsername, String storedPassword, Cache<Credentials, Boolean> authCacheStore, Optional<HashingAlgorithm> hashingAlgo)
    {
        Credentials passSet = new Credentials(inputUsername, inputPassword);
        if (authCacheStore != null) {
            try {
                return authCacheStore.get(passSet, () -> {
                    if (storedUsername.equals(inputUsername) && hashingAlgo.isPresent()) {
                        switch (hashingAlgo.get()) {
                            case BCRYPT:
                                return isBCryptPasswordValid(inputPassword, storedPassword);
                            case PBKDF2:
                                return isPBKDF2PasswordValid(inputPassword, storedPassword);
                            default:
                                return false;
                        }
                    }
                    return false;
                });
            }
            catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }
}
