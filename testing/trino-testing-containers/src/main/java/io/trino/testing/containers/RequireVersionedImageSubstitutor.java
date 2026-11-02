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

import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.ImageNameSubstitutor;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Fails fast when a container would run an unpinned image. Testcontainers invokes the
 * configured substitutor for every image it resolves, so registering this globally (via
 * {@code META-INF/services/org.testcontainers.utility.ImageNameSubstitutor}) enforces the rule
 * across every container in the JVM, not only the images this project defines.
 * <p>
 * Both an untagged reference and an explicit {@code :latest} tag resolve to the version part
 * {@code latest}, which pulls a mutable image and makes runs non-reproducible. Images must instead
 * be pinned to an explicit version tag or digest.
 */
public class RequireVersionedImageSubstitutor
        extends ImageNameSubstitutor
{
    @Override
    public DockerImageName apply(DockerImageName original)
    {
        checkArgument(
                !original.getVersionPart().equals("latest"),
                "Container image must be pinned to an explicit version or digest, not latest or untagged: %s. " +
                        "For the Trino image, set the '%s' system property to the locally built image.",
                original.asCanonicalNameString(),
                TrinoTestImages.TRINO_IMAGE_PROPERTY);
        return original;
    }

    @Override
    protected String getDescription()
    {
        return RequireVersionedImageSubstitutor.class.getSimpleName();
    }
}
