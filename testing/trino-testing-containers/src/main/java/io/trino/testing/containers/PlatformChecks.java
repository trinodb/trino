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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.PullResponseItem;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.ImageNameSubstitutor;

import static com.google.common.base.Strings.padEnd;
import static com.sun.jna.Platform.isARM;
import static java.lang.System.exit;
import static java.lang.System.getenv;
import static java.util.Locale.ENGLISH;

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

        DockerClient client = DockerClientFactory.lazyClient();
        if (!imageExists(client, dockerImageName)) {
            pullImage(client, dockerImageName);
        }

        String imageArch = getImageArch(client, dockerImageName);

        boolean isJavaOnArm = isARM();
        boolean isImageArmBased = imageArch.contains("arm");
        boolean hasIncompatibleRuntime = (isJavaOnArm != isImageArmBased);

        if (hasIncompatibleRuntime) {
            String dockerArch = client.versionCmd().exec().getArch();

            System.err.println("""

                    !!! ERROR !!!
                    Detected incompatible Docker image and host architectures. The performance of running docker images in such scenarios can vary or not work at all.
                    Host: %s (%s).
                    Docker architecture: %s.
                    Docker image architecture: %s.

                    Set environment variable TESTCONTAINERS_SKIP_ARCHITECTURE_CHECK=true to skip this check.
                    !!! ERROR !!!
                    """.formatted(System.getProperty("os.name"), System.getProperty("os.arch"), dockerArch, imageArch));

            exit(99);
        }

        return dockerImageName;
    }

    @Override
    protected String getDescription()
    {
        return "Image substitutor that checks whether the image platform matches host platform";
    }

    private static boolean imageExists(DockerClient client, DockerImageName imageName)
    {
        try {
            getImageArch(client, imageName);
            return true;
        }
        catch (NotFoundException e) {
            return false;
        }
    }

    private static String getImageArch(DockerClient client, DockerImageName imageName)
    {
        return client.inspectImageCmd(imageName.asCanonicalNameString())
                .exec()
                .getArch()
                .toLowerCase(ENGLISH);
    }

    private static void pullImage(DockerClient client, DockerImageName imageName)
    {
        try {
            client.pullImageCmd(imageName.asCanonicalNameString()).exec(new PullImageResultCallback() {
                @Override
                public void onNext(PullResponseItem item)
                {
                    String progress = item.getProgress();
                    if (progress != null) {
                        System.err.println(padEnd(imageName.asCanonicalNameString() + ":" + item.getId(), 50, ' ') + ' ' + progress);
                    }
                }
            }).awaitCompletion();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
