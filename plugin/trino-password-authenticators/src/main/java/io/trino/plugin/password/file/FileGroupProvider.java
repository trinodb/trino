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

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.security.GroupProvider;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_UNAVAILABLE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileGroupProvider
        implements GroupProvider
{
    private static final Splitter LINE_SPLITTER = Splitter.on(":").limit(2).trimResults();
    private static final Splitter GROUP_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private final Supplier<Function<String, Set<String>>> userGroupSupplier;

    @Inject
    public FileGroupProvider(FileGroupConfig config)
    {
        File file = config.getGroupFile();

        userGroupSupplier = memoizeWithExpiration(
                () -> loadGroupFile(file),
                config.getRefreshPeriod().toMillis(),
                MILLISECONDS);
    }

    @Override
    public Set<String> getGroups(String user)
    {
        requireNonNull(user, "user is null");
        return userGroupSupplier.get().apply(user);
    }

    private static Function<String, Set<String>> loadGroupFile(File file)
    {
        Map<String, Set<String>> groups = loadGroupFile(readGroupFile(file));
        return user -> groups.getOrDefault(user, ImmutableSet.of());
    }

    private static Map<String, Set<String>> loadGroupFile(List<String> lines)
    {
        Multimap<String, String> userGroups = HashMultimap.create();
        for (int lineNumber = 1; lineNumber <= lines.size(); lineNumber++) {
            String line = lines.get(lineNumber - 1).trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }

            List<String> parts = LINE_SPLITTER.splitToList(line);
            if (parts.size() != 2) {
                throw invalidFile(lineNumber, "Expected two parts for group and users", null);
            }
            String group = parts.get(0);
            GROUP_SPLITTER.splitToStream(parts.get(1))
                    .forEach(user -> userGroups.put(user, group));
        }
        return userGroups.asMap().entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> ImmutableSet.copyOf(entry.getValue())));
    }

    private static RuntimeException invalidFile(int lineNumber, String message, Throwable cause)
    {
        return new TrinoException(CONFIGURATION_INVALID, format("Error in group file line %s: %s", lineNumber, message), cause);
    }

    private static List<String> readGroupFile(File file)
    {
        try {
            return Files.readAllLines(file.toPath());
        }
        catch (IOException e) {
            throw new TrinoException(CONFIGURATION_UNAVAILABLE, "Failed to read group file: " + file, e);
        }
    }
}
