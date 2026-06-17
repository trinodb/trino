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
package io.trino.execution.admission;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Static-check test that enforces the pick-friendly diff invariant (Property 7)
 * described in the design document.
 *
 * <p>Runs {@code git diff} against the merge base on {@code origin/master} and
 * asserts:
 * <ul>
 *   <li>Four protected engine files have zero-line diffs (Req 5.1–5.4).</li>
 *   <li>{@code LocalDispatchQuery.java} diff matches the call-site-swap
 *       regex allowlist (Req 5.5).</li>
 *   <li>{@code CoordinatorModule.java} diff is exactly one line matching the
 *       {@code binder.install(new AdmissionPolicyModule())} pattern (Req 5.5).</li>
 *   <li>{@code Plugin.java} diff is exactly one default method matching
 *       {@code default Iterable<AdmissionPolicyFactory> getAdmissionPolicyFactories()}
 *       (Req 5.5).</li>
 *   <li>Every newly added {@code .java} file under {@code src/main/} lives
 *       under one of two allowed directory roots (Req 5.6).</li>
 * </ul>
 *
 * <p>Skipped via {@code assumeTrue} when {@code .git} is unavailable in CI.
 *
 * <p>Validates: Requirements 5.1, 5.2, 5.3, 5.4, 5.5, 5.6 /
 * Design: §Test strategy, §Correctness Properties — Property 7
 */
public class TestPickFriendlyDiff
{
    private static final List<String> PROTECTED_FILES = List.of(
            "core/trino-main/src/main/java/io/trino/execution/ClusterSizeMonitor.java",
            "core/trino-main/src/main/java/io/trino/execution/QueryManagerConfig.java",
            "core/trino-main/src/main/java/io/trino/SystemSessionProperties.java",
            "core/trino-main/src/main/java/io/trino/memory/ClusterMemoryManager.java");

    private static final String ALLOWED_SPI_PATH_PREFIX =
            "core/trino-spi/src/main/java/io/trino/spi/admission/";
    private static final String ALLOWED_MAIN_PATH_PREFIX =
            "core/trino-main/src/main/java/io/trino/execution/admission/";

    /**
     * Regex matching the expected single-line addition in CoordinatorModule.java:
     * whitespace followed by {@code binder.install(new AdmissionPolicyModule());}.
     */
    private static final Pattern COORDINATOR_MODULE_LINE_PATTERN =
            Pattern.compile("\\s*binder\\.install\\(new AdmissionPolicyModule\\(\\)\\);");

    /**
     * Regex matching the expected default method addition in Plugin.java.
     */
    private static final Pattern PLUGIN_DEFAULT_METHOD_PATTERN =
            Pattern.compile("default Iterable<AdmissionPolicyFactory> getAdmissionPolicyFactories\\(\\)");

    /**
     * Regex allowlist for the LocalDispatchQuery.java diff — the call-site-swap
     * replacing the unconditional {@code clusterSizeMonitor.waitForMinimumWorkers}
     * with the admission policy invocation and switch dispatch.
     */
    private static final List<Pattern> LOCAL_DISPATCH_QUERY_ALLOWLIST = List.of(
            // Import additions for admission types
            Pattern.compile(".*import io\\.trino\\.spi\\.admission\\..*"),
            // The admission policy field / injection
            Pattern.compile(".*admissionPolicy.*"),
            Pattern.compile(".*AdmissionPolicy.*"),
            // The buildAdmissionContext helper method
            Pattern.compile(".*buildAdmissionContext.*"),
            Pattern.compile(".*QueryAdmissionContext.*"),
            // The shouldQueryWait invocation
            Pattern.compile(".*shouldQueryWait.*"),
            // The WaitDecision switch and branches
            Pattern.compile(".*WaitDecision.*"),
            Pattern.compile(".*ProceedNow.*"),
            Pattern.compile(".*Wait\\s+wait.*"),
            // The insufficientResources helper
            Pattern.compile(".*insufficientResources.*"),
            // The instanceof check for default policy
            Pattern.compile(".*instanceof MinWorkersAdmissionPolicy.*"),
            // Decision variable
            Pattern.compile(".*decision.*"),
            // Blank/brace-only lines and comments
            Pattern.compile("\\s*"),
            Pattern.compile("\\s*[{}]\\s*"),
            Pattern.compile("\\s*//.*"),
            // The switch/case/arrow syntax lines
            Pattern.compile(".*switch\\s*\\(.*"),
            Pattern.compile(".*case\\s+.*->.*"),
            // try/catch for policy invocation
            Pattern.compile(".*try\\s*\\{.*"),
            Pattern.compile(".*catch\\s*\\(Throwable.*"),
            Pattern.compile(".*return;.*"),
            // Timeout variable for Wait branch
            Pattern.compile(".*Duration timeout.*"),
            // getRequiredWorkersMaxWait reference
            Pattern.compile(".*getRequiredWorkersMaxWait.*"),
            // startExecution call (moved into switch branch)
            Pattern.compile(".*startExecution.*"),
            // Standard control flow elements
            Pattern.compile(".*if\\s*\\(.*"),
            Pattern.compile(".*transitionToFailed.*"),
            // TrinoException / StandardErrorCode additions
            Pattern.compile(".*TrinoException.*"),
            Pattern.compile(".*StandardErrorCode.*"),
            Pattern.compile(".*GENERIC_INSUFFICIENT_RESOURCES.*"),
            // ctx variable
            Pattern.compile(".*ctx.*"));

    private static Path repoRoot;

    @BeforeAll
    static void verifyGitAvailable()
            throws Exception
    {
        // Skip the entire test class if .git is unavailable (e.g. in some CI envs)
        Path currentDir = Path.of("").toAbsolutePath();
        Path candidate = currentDir;
        while (candidate != null) {
            if (Files.isDirectory(candidate.resolve(".git"))) {
                repoRoot = candidate;
                break;
            }
            candidate = candidate.getParent();
        }
        assumeTrue(repoRoot != null, ".git directory not found; skipping pick-friendly diff check");

        // Also verify git command is available
        try {
            Process process = new ProcessBuilder("git", "--version")
                    .directory(repoRoot.toFile())
                    .redirectErrorStream(true)
                    .start();
            int exitCode = process.waitFor();
            assumeTrue(exitCode == 0, "git command not available; skipping pick-friendly diff check");
        }
        catch (IOException e) {
            assumeTrue(false, "git command not available; skipping pick-friendly diff check");
        }
    }

    @Test
    @DisplayName("Protected engine files have zero-line diff against origin/master (Req 5.1–5.4)")
    public void protectedFilesHaveZeroDiff()
            throws Exception
    {
        String mergeBase = getMergeBase();

        for (String protectedFile : PROTECTED_FILES) {
            String diff = gitDiff(mergeBase, protectedFile);
            assertThat(diff)
                    .as("Protected file %s must have zero diff against origin/master", protectedFile)
                    .isEmpty();
        }
    }

    @Test
    @DisplayName("LocalDispatchQuery.java diff matches call-site-swap allowlist (Req 5.5)")
    public void localDispatchQueryDiffMatchesAllowlist()
            throws Exception
    {
        String mergeBase = getMergeBase();
        String diff = gitDiff(mergeBase, "core/trino-main/src/main/java/io/trino/dispatcher/LocalDispatchQuery.java");

        if (diff.isEmpty()) {
            // No changes to LocalDispatchQuery yet — acceptable if the feature hasn't been applied
            return;
        }

        // Extract added/removed content lines (skip diff metadata lines starting with @@, ---, +++)
        List<String> contentLines = diff.lines()
                .filter(line -> (line.startsWith("+") || line.startsWith("-"))
                        && !line.startsWith("+++")
                        && !line.startsWith("---"))
                .map(line -> line.substring(1)) // strip the leading +/-
                .filter(line -> !line.isBlank()) // ignore blank lines
                .toList();

        for (String line : contentLines) {
            boolean matched = LOCAL_DISPATCH_QUERY_ALLOWLIST.stream()
                    .anyMatch(pattern -> pattern.matcher(line).matches());
            assertThat(matched)
                    .as("LocalDispatchQuery.java diff line not in allowlist: '%s'", line)
                    .isTrue();
        }
    }

    @Test
    @DisplayName("CoordinatorModule.java diff is exactly one binder.install line (Req 5.5)")
    public void coordinatorModuleDiffIsExactlyOneBinderInstallLine()
            throws Exception
    {
        String mergeBase = getMergeBase();
        String diff = gitDiff(mergeBase, "core/trino-main/src/main/java/io/trino/server/CoordinatorModule.java");

        if (diff.isEmpty()) {
            // No changes yet — acceptable if the feature hasn't been applied
            return;
        }

        // Extract only added lines (non-metadata)
        List<String> addedLines = diff.lines()
                .filter(line -> line.startsWith("+") && !line.startsWith("+++"))
                .map(line -> line.substring(1))
                .filter(line -> !line.isBlank())
                .toList();

        assertThat(addedLines)
                .as("CoordinatorModule.java should have exactly one non-blank added line")
                .hasSize(1);
        assertThat(COORDINATOR_MODULE_LINE_PATTERN.matcher(addedLines.getFirst()).matches())
                .as("CoordinatorModule.java added line must match binder.install(new AdmissionPolicyModule()); pattern, got: '%s'", addedLines.getFirst())
                .isTrue();
    }

    @Test
    @DisplayName("Plugin.java diff is exactly one default method (Req 5.5)")
    public void pluginJavaDiffIsExactlyOneDefaultMethod()
            throws Exception
    {
        String mergeBase = getMergeBase();
        String diff = gitDiff(mergeBase, "core/trino-spi/src/main/java/io/trino/spi/Plugin.java");

        if (diff.isEmpty()) {
            // No changes yet — acceptable if the feature hasn't been applied
            return;
        }

        // Extract added lines (non-metadata, non-blank)
        List<String> addedLines = diff.lines()
                .filter(line -> line.startsWith("+") && !line.startsWith("+++"))
                .map(line -> line.substring(1))
                .filter(line -> !line.isBlank())
                .toList();

        // The diff should contain the default method signature
        boolean hasMethodSignature = addedLines.stream()
                .anyMatch(line -> PLUGIN_DEFAULT_METHOD_PATTERN.matcher(line).find());
        assertThat(hasMethodSignature)
                .as("Plugin.java diff must contain the getAdmissionPolicyFactories() default method signature")
                .isTrue();

        // Verify it's a single method addition: look for exactly one method signature
        long methodSignatureCount = addedLines.stream()
                .filter(line -> line.contains("default ") && line.contains("("))
                .count();
        assertThat(methodSignatureCount)
                .as("Plugin.java diff should contain exactly one default method addition")
                .isEqualTo(1L);
    }

    @Test
    @DisplayName("All newly added .java files under src/main/ are in allowed directories (Req 5.6)")
    public void newlyAddedFilesAreInAllowedDirectories()
            throws Exception
    {
        String mergeBase = getMergeBase();

        // List newly added files (status 'A' in diff)
        List<String> addedFiles = gitDiffNameStatus(mergeBase);

        List<String> newMainJavaFiles = addedFiles.stream()
                .filter(file -> file.endsWith(".java"))
                .filter(file -> file.contains("src/main/"))
                .toList();

        for (String file : newMainJavaFiles) {
            boolean inAllowedDir = file.startsWith(ALLOWED_SPI_PATH_PREFIX)
                    || file.startsWith(ALLOWED_MAIN_PATH_PREFIX);
            assertThat(inAllowedDir)
                    .as("Newly added file '%s' must be under '%s' or '%s'",
                            file, ALLOWED_SPI_PATH_PREFIX, ALLOWED_MAIN_PATH_PREFIX)
                    .isTrue();
        }
    }

    // ---- Git helpers ----

    private static String getMergeBase()
            throws Exception
    {
        // Fetch origin/master if not already available (best effort, don't fail if remote is missing)
        try {
            runGit("fetch", "origin", "master");
        }
        catch (Exception _) {
            // If fetch fails (e.g., offline), try with what's available locally
        }

        String mergeBase = runGit("merge-base", "HEAD", "origin/master").trim();
        assumeTrue(!mergeBase.isBlank(), "Could not determine merge base with origin/master");
        return mergeBase;
    }

    private static String gitDiff(String mergeBase, String filePath)
            throws Exception
    {
        return runGit("diff", mergeBase, "--", filePath);
    }

    private static List<String> gitDiffNameStatus(String mergeBase)
            throws Exception
    {
        String output = runGit("diff", "--name-status", "--diff-filter=A", mergeBase);
        if (output.isBlank()) {
            return List.of();
        }
        return output.lines()
                .map(line -> {
                    // Format: "A\tpath/to/file.java"
                    String[] parts = line.split("\t", 2);
                    return parts.length == 2 ? parts[1] : line;
                })
                .toList();
    }

    private static String runGit(String... args)
            throws Exception
    {
        String[] command = Stream.concat(Stream.of("git", "-P"), Stream.of(args))
                .toArray(String[]::new);
        Process process = new ProcessBuilder(command)
                .directory(repoRoot.toFile())
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes());
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("git command failed (exit " + exitCode + "): " + String.join(" ", command) + "\n" + output);
        }
        return output;
    }
}
