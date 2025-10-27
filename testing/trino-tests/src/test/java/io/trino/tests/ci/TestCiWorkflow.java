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

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.util.Locale.ENGLISH;
import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCiWorkflow
{
    private static final Logger log = Logger.get(TestCiWorkflow.class);

    private static final Path CI_YML_REPO_PATH = Paths.get(".github/workflows/ci.yml");
    private static final String BUILD_SUCCESS = "build-success";

    @Test
    public void testUploadTestResultsCondition()
            throws Exception
    {
        String uploadTestResultsStepName = "Upload test results";
        Set<String> nonTestSteps = ImmutableSet.of(
                uploadTestResultsStepName,
                "Maven Install");

        Yaml yaml = new Yaml();
        Map<?, ?> workflow = yaml.load(new StringReader(Files.readString(findRepositoryRoot().resolve(CI_YML_REPO_PATH))));
        Map<String, ?> jobs = getMap(workflow, "jobs");
        Map<String, ?> test = getMap(jobs, "test");
        List<?> steps = getList(test, "steps");

        List<Map<?, ?>> allTestSteps = findAll(steps, (Map<?, ?> step) -> {
            if (step.containsKey("name") && nonTestSteps.contains(getString(step, "name"))) {
                return false;
            }
            boolean runsTests = false;
            if (step.containsKey("run")) {
                String run = getString(step, "run").toLowerCase(ENGLISH);
                runsTests = run.contains("maven") || run.contains("test");
            }
            boolean looksLikeTests = step.containsKey("name") && getString(step, "name").toLowerCase(ENGLISH).contains("test");
            return runsTests || looksLikeTests;
        });

        @SuppressWarnings({"unchecked", "rawtypes"})
        List<String> testStepIds = allTestSteps.stream()
                // Each test step must have an ID
                .peek(testStep -> assertThat((Map) testStep).containsKey("id"))
                .map(testStep -> getString(testStep, "id"))
                .collect(toImmutableList());

        assertThat(testStepIds)
                .doesNotHaveDuplicates();

        Map<?, ?> uploadTestResults = findOnly(steps, (Map<?, ?> step) -> Objects.equals(step.get("name"), uploadTestResultsStepName));
        System.out.println(getString(getMap(uploadTestResults, "with"), "has-failed-tests"));
        Set<String> conditions = Stream.of(getString(getMap(uploadTestResults, "with"), "has-failed-tests")
                        .strip()
                        .replaceFirst("^\\$\\{\\{", "")
                        .replaceFirst("}}$", "")
                        .strip()
                        .split("\\|\\|"))
                .map(option -> option.strip().replaceFirst("steps\\.([-a-zA-Z0-9]+)\\.outcome == 'failure'", "$1"))
                .collect(toImmutableSet());

        assertThat(conditions)
                .containsExactlyInAnyOrderElementsOf(testStepIds);
    }

    @Test
    public void testBuildSuccessDependencies()
            throws Exception
    {
        Yaml yaml = new Yaml();
        Map<?, ?> workflow = yaml.load(new StringReader(Files.readString(findRepositoryRoot().resolve(CI_YML_REPO_PATH))));
        Map<String, ?> jobs = getMap(workflow, "jobs");

        Set<String> allJobNames = jobs.keySet();
        assertThat(allJobNames).as("allJobNames").contains(BUILD_SUCCESS);
        List<String> testJobNames = allJobNames.stream()
                .filter(not(BUILD_SUCCESS::equals))
                .sorted()
                .toList();
        Map<?, ?> buildSuccessJob = getMap(jobs, BUILD_SUCCESS);

        List<String> buildSuccessDependencies = getList(buildSuccessJob, "needs").stream()
                .map(String.class::cast)
                .toList();
        assertThat(buildSuccessDependencies).as("dependencies for %s", BUILD_SUCCESS)
                .isSorted()
                .doesNotHaveDuplicates()
                .containsExactlyElementsOf(testJobNames);

        StringBuilder expectedRunDefinition = new StringBuilder();
        expectedRunDefinition.append("# generated by %s\n".formatted(getClass().getSimpleName()));
        for (String name : testJobNames) {
            expectedRunDefinition.append(
                    "echo '${{ needs.%1$s.result }}' | grep -xE 'success|skipped' || { echo 'Job \"%1$s\" failed' >&2; exit 1; }\n".formatted(name));
        }
        Map<?, ?> runDefinition = getList(buildSuccessJob, "steps").stream()
                .map(step -> (Map<?, ?>) step)
                .filter(step -> "Check results".equals(step.get("name")))
                .collect(onlyElement());
        assertThat(runDefinition.get("run")).as("run script")
                .isEqualTo(expectedRunDefinition.toString());
    }

    @Test
    public void testBuildSuccessIsLast()
            throws Exception
    {
        Yaml yaml = new Yaml();
        Map<?, ?> workflow = yaml.load(new StringReader(Files.readString(findRepositoryRoot().resolve(CI_YML_REPO_PATH))));
        Map<String, ?> jobs = getMap(workflow, "jobs");
        // This assumes the `jobs` map preserves source order
        assertThat(getLast(jobs.keySet()))
                .describedAs("The %s job is logically last and depends on all others, we want it to be last in the source file", BUILD_SUCCESS)
                .isEqualTo(BUILD_SUCCESS);
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

    @SuppressWarnings("unchecked")
    private static <T> T findOnly(List<?> list, Predicate<T> predicate)
    {
        return (T) list.stream()
                .filter(e -> predicate.test((T) e))
                .collect(onlyElement());
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> findAll(List<?> list, Predicate<T> predicate)
    {
        return (List<T>) list.stream()
                .filter(e -> predicate.test((T) e))
                .collect(toImmutableList());
    }
}
