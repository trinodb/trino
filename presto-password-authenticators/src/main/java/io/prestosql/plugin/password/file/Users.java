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

import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.prestosql.spi.StandardErrorCode.CONFIGURATION_INVALID;

public class Users
{
    private static final Logger log = Logger.get(Users.class);
    private final Set<User> users;

    public Users(Set<User> users)
    {
        this.users = users;
    }

    public Set<User> getUsers()
    {
        return users;
    }

    public static Map<String, User> readUsers(
            String name,
            int bcryptMinCost,
            int pbkdf2MinIterations,
            Cache<Credentials, Boolean> bcryptCache,
            Cache<Credentials, Boolean> pbkdf2Cache)
            throws IOException
    {
        try (Stream<String> stream = Files.lines(Paths.get(name))) {
            return stream.map(String::trim)
                    .filter(line -> !line.isEmpty())
                    .map(line -> Splitter.on(":").limit(2).splitToList(line))
                    .filter(words -> {
                        if (words.size() == 2) {
                            return true;
                        }
                        else {
                            throw new PrestoException(CONFIGURATION_INVALID, "Invalid User Password input");
                        }
                    })
                    .collect(Collectors.toMap(words -> words.get(0),
                            words -> new User(words.get(0), words.get(1), bcryptCache, pbkdf2Cache, bcryptMinCost, pbkdf2MinIterations)));
        }
        catch (IOException e) {
            throw e;
        }
    }
}
