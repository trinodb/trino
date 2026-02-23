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
package io.trino.tests.product.launcher.env;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.inject.ConfigurationException;
import com.google.inject.Inject;
import com.google.inject.spi.Message;
import info.debatty.java.stringsimilarity.JaroWinkler;
import info.debatty.java.stringsimilarity.interfaces.StringSimilarity;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tests.product.launcher.Configurations.canonicalEnvironmentName;
import static java.util.Comparator.comparingDouble;
import static java.util.Objects.requireNonNull;

public final class EnvironmentFactory
{
    private static final StringSimilarity SIMILARITY = new JaroWinkler();

    private final Map<String, EnvironmentProvider> environmentProviders;

    @Inject
    public EnvironmentFactory(Map<String, EnvironmentProvider> environmentProviders)
    {
        this.environmentProviders = requireNonNull(environmentProviders, "environmentProviders is null");
    }

    public Environment.Builder get(String environmentName, PrintStream printStream, EnvironmentConfig config, Map<String, String> extraOptions)
    {
        environmentName = canonicalEnvironmentName(environmentName);
        if (!environmentProviders.containsKey(environmentName)) {
            throw new ConfigurationException(ImmutableList.of(new Message("No environment with name '%s'%s".formatted(environmentName, suggest(environmentName, environmentProviders.keySet())))));
        }
        return environmentProviders.get(environmentName)
                .createEnvironment(environmentName, printStream, config, extraOptions);
    }

    public List<String> list()
    {
        return Ordering.natural().sortedCopy(environmentProviders.keySet());
    }

    private static String suggest(String environmentName, Set<String> allEnvironments)
    {
        List<String> suggestions = findSimilar(environmentName, allEnvironments, 3);
        if (suggestions.isEmpty()) {
            return "";
        }

        return ". Did you mean to use " + switch (suggestions.size()) {
            case 3 -> "'" + suggestions.get(0) + "', '" + suggestions.get(1) + "' or '" + suggestions.get(2) + "'?";
            case 2 -> "'" + suggestions.get(0) + "' or '" + suggestions.get(1) + "'?";
            default -> "'" + suggestions.get(0) + "'?";
        };
    }

    private static List<String> findSimilar(String environmentName, Set<String> allEnvironments, int count)
    {
        return allEnvironments.stream()
                .map(candidate -> new Match(candidate, SIMILARITY.similarity(environmentName, candidate)))
                .filter(match -> match.ratio() > 0.5)
                .sorted(comparingDouble(Match::ratio).reversed())
                .limit(count)
                .map(Match::environment)
                .collect(toImmutableList());
    }

    private record Match(String environment, double ratio)
    {
        public Match
        {
            requireNonNull(environment, "environment is null");
            verify(ratio >= 0.0 && ratio <= 1.0, "ratio must be in the [0, 1.0] range");
        }
    }
}
