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
package io.trino.plugin.password.file;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableMap;
import io.trino.collect.cache.NonEvictableLoadingCache;
import io.trino.plugin.password.Credential;
import io.trino.spi.TrinoException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.password.file.EncryptionUtil.doesBCryptPasswordMatch;
import static io.trino.plugin.password.file.EncryptionUtil.doesPBKDF2PasswordMatch;
import static io.trino.plugin.password.file.EncryptionUtil.getHashingAlgorithm;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_UNAVAILABLE;
import static java.lang.String.format;

public class PasswordStore
{
    private static final Splitter LINE_SPLITTER = Splitter.on(":").limit(2);

    private final Map<String, HashedPassword> credentials;
    private final NonEvictableLoadingCache<Credential, Boolean> cache;

    public PasswordStore(File file, int cacheMaxSize)
    {
        this(readPasswordFile(file), cacheMaxSize);
    }

    @VisibleForTesting
    public PasswordStore(List<String> lines, int cacheMaxSize)
    {
        credentials = loadPasswordFile(lines);
        cache = buildNonEvictableCache(
                CacheBuilder.newBuilder().maximumSize(cacheMaxSize),
                CacheLoader.from(this::matches));
    }

    public boolean authenticate(String user, String password)
    {
        return cache.getUnchecked(new Credential(user, password));
    }

    private boolean matches(Credential credential)
    {
        HashedPassword hashed = credentials.get(credential.getUser());
        return (hashed != null) && hashed.matches(credential.getPassword());
    }

    private static Map<String, HashedPassword> loadPasswordFile(List<String> lines)
    {
        Map<String, HashedPassword> users = new HashMap<>();
        for (int lineNumber = 1; lineNumber <= lines.size(); lineNumber++) {
            String line = lines.get(lineNumber - 1).trim();
            if (line.isEmpty()) {
                continue;
            }

            List<String> parts = LINE_SPLITTER.splitToList(line);
            if (parts.size() != 2) {
                throw invalidFile(lineNumber, "Expected two parts for user and password", null);
            }
            String user = parts.get(0);
            String password = parts.get(1);

            try {
                if (users.put(user, getHashedPassword(password)) != null) {
                    throw invalidFile(lineNumber, "Duplicate user: " + user, null);
                }
            }
            catch (HashedPasswordException e) {
                throw invalidFile(lineNumber, e.getMessage(), e);
            }
        }
        return ImmutableMap.copyOf(users);
    }

    private static RuntimeException invalidFile(int lineNumber, String message, Throwable cause)
    {
        return new TrinoException(CONFIGURATION_INVALID, format("Error in password file line %s: %s", lineNumber, message), cause);
    }

    private static List<String> readPasswordFile(File file)
    {
        try {
            return Files.readAllLines(file.toPath());
        }
        catch (IOException e) {
            throw new TrinoException(CONFIGURATION_UNAVAILABLE, "Failed to read password file: " + file, e);
        }
    }

    private static HashedPassword getHashedPassword(String hashedPassword)
    {
        switch (getHashingAlgorithm(hashedPassword)) {
            case BCRYPT:
                return password -> doesBCryptPasswordMatch(password, hashedPassword);
            case PBKDF2:
                return password -> doesPBKDF2PasswordMatch(password, hashedPassword);
        }
        throw new HashedPasswordException("Hashing algorithm of password cannot be determined");
    }

    public interface HashedPassword
    {
        boolean matches(String password);
    }
}
