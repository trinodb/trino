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
import com.google.common.cache.CacheBuilder;
import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.BasicPrincipal;
import io.prestosql.spi.security.PasswordAuthenticator;

import javax.inject.Inject;

import java.io.IOException;
import java.security.Principal;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static io.prestosql.plugin.password.file.Users.readUsers;
import static io.prestosql.spi.StandardErrorCode.CONFIGURATION_INVALID;

public class FileBasedAuthenticator
        implements PasswordAuthenticator
{
    private static final Logger log = Logger.get(FileBasedAuthenticator.class);
    private Cache<Credentials, Boolean> bcryptCache;
    private Cache<Credentials, Boolean> pbkdf2Cache;
    private Supplier<Map<String, User>> readUserSupplier;
    private long expirationTimeForSupplier;
    private TimeUnit expirationTimeForSupplierTimeUnit;

    @Inject
    public FileBasedAuthenticator(FileBasedAuthenticationConfig config)
    {
        if (config.getRefreshPeriod().toMillis() != 0) {
            // create a supplier which refreshes every config.getRefreshPeriod()
            expirationTimeForSupplier = config.getRefreshPeriod().toMillis();
            expirationTimeForSupplierTimeUnit = TimeUnit.MILLISECONDS;
        }
        else {
            // create a supplier which never refreshes
            expirationTimeForSupplier = Long.MAX_VALUE;
            expirationTimeForSupplierTimeUnit = TimeUnit.DAYS;
        }

        readUserSupplier = memoizeWithExpiration(
                () -> {
                    log.debug("Refreshing FileBasedAuthFile from %s", config.getConfigFile());
                    try {
                        bcryptCache = CacheBuilder.newBuilder()
                                .concurrencyLevel(1)
                                .maximumSize(config.getAuthTokenCacheStoreMaxSize())
                                .build();

                        pbkdf2Cache = CacheBuilder.newBuilder()
                                .concurrencyLevel(1)
                                .maximumSize(config.getAuthTokenCacheStoreMaxSize())
                                .build();
                        return readUsers(config.getConfigFile(), config.getBcryptMinCost(), config.getPbkdf2MinIterations(), bcryptCache, pbkdf2Cache);
                    }
                    catch (IOException e) {
                        throw new PrestoException(CONFIGURATION_INVALID, "Failed to read password database: " + config.getConfigFile(), e);
                    }
                },
                expirationTimeForSupplier,
                expirationTimeForSupplierTimeUnit);
    }

    private User getUserProfile(String inputUsername)
    {
        return readUserSupplier.get().get(inputUsername);
    }

    @Override
    public Principal createAuthenticatedPrincipal(String inputUsername, String inputPassword)
    {
        User user = getUserProfile(inputUsername);
        if (user == null || !user.authenticate(inputUsername, inputPassword)) {
            throw new AccessDeniedException("Invalid credentials");
        }

        return new BasicPrincipal(inputUsername);
    }
}
