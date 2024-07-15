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
package io.trino.tests.ci;

import io.airlift.log.Logger;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class TestWorkflows
{
    private static final Logger log = Logger.get(TestWorkflows.class);

    @Test
    public void testWorkflows()
            throws Exception
    {
        List<String> errors = new ArrayList<>();
        listWorkflows().forEach(path -> errors.addAll(checkWorkflow(path)));
        if (!errors.isEmpty()) {
            fail("Errors: " + errors.stream()
                    .map("\n\t\t%s"::formatted)
                    .collect(joining()));
        }
    }

    private static List<Path> listWorkflows()
            throws Exception
    {
        try (Stream<Path> walk = Files.walk(findRepositoryRoot().resolve(".github/workflows"), 1)) {
            return walk
                    .filter(path -> path.toString().endsWith(".yml"))
                    .collect(toImmutableList());
        }
    }

    private static List<String> checkWorkflow(Path path)
    {
        List<String> errors = new ArrayList<>();
        try {
            Yaml yaml = new Yaml();
            Map<?, ?> workflow = yaml.load(new StringReader(Files.readString(path)));
            Map<String, ?> jobs = getMap(workflow, "jobs");
            jobs.forEach((jobName, jobDefinition) -> {
                Map<?, ?> job = (Map<?, ?>) jobDefinition;

                List<?> steps = getList(job, "steps");
                for (int stepPosition = 0; stepPosition < steps.size(); stepPosition++) {
                    Map<?, ?> step = (Map<?, ?>) steps.get(stepPosition);
                    String stepName = firstNonNull((String) step.get("name"), "Step #" + stepPosition);
                    if (step.containsKey("uses")) {
                        String uses = getString(step, "uses");
                        if (!isSafeActionReference(uses)) {
                            errors.add("Unsafe action reference in %s » %s » %s: %s. A reference to a 3rd party action is safe when it pins to a specific commit.".formatted(
                                    path, jobName, stepName, uses));
                        }
                    }
                }
            });
        }
        catch (AssertionError | Exception e) {
            throw new AssertionError("Failed when checking %s: %s".formatted(path, e), e);
        }
        return errors;
    }

    @Test
    public void testActions()
            throws Exception
    {
        List<String> errors = new ArrayList<>();
        listActions().forEach(path -> errors.addAll(checkAction(path)));
        if (!errors.isEmpty()) {
            fail("Errors: " + errors.stream()
                    .map("\n\t\t%s"::formatted)
                    .collect(joining()));
        }
    }

    private static List<Path> listActions()
            throws Exception
    {
        Path actionsDir = findRepositoryRoot().resolve(".github/actions");
        try (Stream<Path> walk = Files.walk(actionsDir, 1)) {
            return walk
                    .filter(Predicate.not(actionsDir::equals))
                    .map(path -> path.resolve("action.yml"))
                    .collect(toImmutableList());
        }
    }

    private static List<String> checkAction(Path path)
    {
        List<String> errors = new ArrayList<>();
        try {
            Yaml yaml = new Yaml();
            Map<?, ?> workflow = yaml.load(new StringReader(Files.readString(path)));
            Map<String, ?> runs = getMap(workflow, "runs");
            assertThat(getString(runs, "using")).isEqualTo("composite"); // test only supports composite actions
            List<?> steps = getList(runs, "steps");
            for (int stepPosition = 0; stepPosition < steps.size(); stepPosition++) {
                Map<?, ?> step = (Map<?, ?>) steps.get(stepPosition);
                String stepName = firstNonNull((String) step.get("name"), "Step #" + stepPosition);
                if (step.containsKey("uses")) {
                    String uses = getString(step, "uses");
                    if (!isSafeActionReference(uses)) {
                        errors.add("Unsafe action reference in %s » %s: %s. A reference to a 3rd party action is safe when it pins to a specific commit.".formatted(
                                path, stepName, uses));
                    }
                }
            }
        }
        catch (AssertionError | Exception e) {
            throw new AssertionError("Failed when checking %s: %s".formatted(path, e), e);
        }
        return errors;
    }

    private static boolean isSafeActionReference(String actionName)
    {
        if (actionName.startsWith("actions/")) {
            // standard action
            return true;
        }
        if (actionName.startsWith("./")) {
            // inline action
            return true;
        }
        if (actionName.startsWith("trinodb/")) {
            // our own
            return true;
        }
        if (actionName.matches(".*@[0-9a-f]{40}$")) {
            // Github disallows 40-character long hex-like strings as branch or tag names, so this must be a commit hash
            // It's immutable and can't be changed by the owner of the repository.
            return true;
        }
        return false;
    }

    private static Path findRepositoryRoot()
    {
        Path workingDirectory = Paths.get("").toAbsolutePath();
        log.info("Current working directory: %s", workingDirectory);
        for (Path path = workingDirectory; path != null; path = path.getParent()) {
            if (Files.isDirectory(path.resolve(".git"))) {
                return path;
            }
        }
        throw new RuntimeException("Failed to find repository root from " + workingDirectory);
    }

    private static String getString(Map<?, ?> map, String key)
    {
        Object value = map.get(key);
        verifyNotNull(value, "No or null entry for key [%s] in %s", key, map);
        return (String) value;
    }

    private static List<?> getList(Map<?, ?> map, String key)
    {
        Object value = map.get(key);
        verifyNotNull(value, "No or null entry for key [%s] in %s", key, map);
        return (List<?>) value;
    }

    private static Map<String, ?> getMap(Map<?, ?> map, String key)
    {
        Object value = map.get(key);
        verifyNotNull(value, "No or null entry for key [%s] in %s", key, map);
        return ((Map<?, ?>) value).entrySet().stream()
                .collect(toImmutableMap(e -> (String) e.getKey(), Map.Entry::getValue));
    }
}
