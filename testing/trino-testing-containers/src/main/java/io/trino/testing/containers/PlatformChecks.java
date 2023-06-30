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
package io.trino.testing.containers;

import io.trino.testing.containers.TestContainers.DockerArchitectureInfo;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.ImageNameSubstitutor;

import static com.sun.jna.Platform.isARM;
import static io.trino.testing.containers.TestContainers.DockerArchitecture.ARM64;
import static io.trino.testing.containers.TestContainers.getDockerArchitectureInfo;
import static java.lang.System.exit;
import static java.lang.System.getenv;

public class PlatformChecks
        extends ImageNameSubstitutor
{
    private static final boolean TESTCONTAINERS_SKIP_ARCH_CHECK = "true".equalsIgnoreCase(getenv("TESTCONTAINERS_SKIP_ARCHITECTURE_CHECK"));

    @Override
    public DockerImageName apply(DockerImageName dockerImageName)
    {
        if (TESTCONTAINERS_SKIP_ARCH_CHECK) {
            return dockerImageName;
        }

        DockerArchitectureInfo architecture = getDockerArchitectureInfo(dockerImageName);

        boolean isJavaOnArm = isARM();
        boolean isImageArmBased = architecture.imageArch().equals(ARM64);
        boolean hasIncompatibleRuntime = (isJavaOnArm != isImageArmBased);

        if (hasIncompatibleRuntime) {
            System.err.println("""

                    !!! ERROR !!!
                    Detected incompatible Docker image and host architectures. The performance of running docker images in such scenarios can vary or not work at all.
                    Host: %s (%s).
                    Docker architecture: %s.
                    Docker image architecture: %s.

                    Set environment variable TESTCONTAINERS_SKIP_ARCHITECTURE_CHECK=true to skip this check.
                    !!! ERROR !!!
                    """.formatted(System.getProperty("os.name"), System.getProperty("os.arch"), architecture.hostArch(), architecture.imageArch()));

            exit(99);
        }

        return dockerImageName;
    }

    @Override
    protected String getDescription()
    {
        return "Image substitutor that checks whether the image platform matches host platform";
    }
}
