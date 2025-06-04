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
package io.trino.tests;

import io.airlift.log.Logger;
import org.apache.maven.model.Dependency;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.joining;

public class TestTestSetup
{
    private static final Logger log = Logger.get(TestTestSetup.class);

    private static final Path ROOT_POM_REPO_PATH = Paths.get("pom.xml");

    @Test
    public void testSetupOfTests()
            throws Exception
    {
        Path rootPom = findRepositoryRoot().resolve(ROOT_POM_REPO_PATH);
        List<String> errors = verifyModule(rootPom);
        if (!errors.isEmpty()) {
            Assertions.fail("Errors: " + errors.stream()
                    .map("\n\t\t%s"::formatted)
                    .collect(joining()));
        }
    }

    private List<String> verifyModule(Path pom)
            throws Exception
    {
        System.out.println("verifying " + pom);

        List<String> errors = new ArrayList<>();
        try (InputStream inputStream = Files.newInputStream(pom)) {
            MavenXpp3Reader reader = new MavenXpp3Reader();
            Model model = reader.read(inputStream);

            boolean hasJunitApi = false;
            boolean hasAirliftJunitExtensions = false;
            for (Dependency dependency : model.getDependencies()) {
                if (dependency.getGroupId().equals("org.junit.jupiter") && dependency.getArtifactId().equals("junit-jupiter-api")) {
                    hasJunitApi = true;
                }
                if (dependency.getGroupId().equals("io.airlift") && dependency.getArtifactId().equals("junit-extensions")) {
                    hasAirliftJunitExtensions = true;
                }
            }
            if (hasJunitApi && !hasAirliftJunitExtensions) {
                errors.add("Missing dependency on io.airlift:junit-extensions in " + pom);
            }

            for (String module : model.getModules()) {
                errors.addAll(verifyModule(pom.resolveSibling(module).resolve("pom.xml")));
            }
        }
        return errors;
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
}
