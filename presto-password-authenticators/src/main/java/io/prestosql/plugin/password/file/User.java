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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.plugin.password.file.EncryptionUtil.checkAndGetHashing;
import static io.prestosql.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.util.Objects.requireNonNull;

public class User
{
    private static final Logger log = Logger.get(User.class);
    private final String name;
    private final String password;
    private final Optional<HashingAlgorithm> hashingAlgo;
    private final Cache<Credentials, Boolean> authCacheStore;

    public User(String name, String password, Cache<Credentials, Boolean> bcryptCache, Cache<Credentials, Boolean> pbkdf2Cache, int bcryptMinCost, int pbkdf2MinIterations)
            throws HashedPasswordException
    {
        this.name = requireNonNull(name, "User name is empty");
        requireNonNull(password, "User name is empty");

        this.password = password;

        hashingAlgo = Optional.of(checkAndGetHashing(password, bcryptMinCost, pbkdf2MinIterations));
        switch (hashingAlgo.get()) {
            case BCRYPT:
                authCacheStore = bcryptCache;
                break;
            case PBKDF2:
                authCacheStore = pbkdf2Cache;
                break;
            default:
                throw new PrestoException(CONFIGURATION_INVALID, "Hashing algorithm of password, stored in the {user:password} file cannot be determined!");
        }
    }

    public String getName()
    {
        return name;
    }

    public String getPassword()
    {
        return password;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("user", name)
                .add("password", password)
                .toString();
    }

    @VisibleForTesting
    public boolean authenticate(String inputUsername, String inputPassword)
    {
        if (authCacheStore != null) {
            return PasswordValidator.authenticate(inputUsername, inputPassword, getName(), getPassword(), authCacheStore, hashingAlgo);
        }
        return false;
    }
}
