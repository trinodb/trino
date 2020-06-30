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
package io.prestosql.tests.product.launcher.env;

import com.google.common.base.Stopwatch;
import io.airlift.log.Logger;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.SelinuxContext;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DockerContainer
        extends FixedHostPortGenericContainer<DockerContainer>
{
    private static final Logger log = Logger.get(DockerContainer.class);

    public DockerContainer(String dockerImageName)
    {
        super(dockerImageName);

        // workaround for https://github.com/testcontainers/testcontainers-java/pull/2861
        setCopyToFileContainerPathMap(new LinkedHashMap<>());
    }

    @Override
    public void addFileSystemBind(String hostPath, String containerPath, BindMode mode)
    {
        verifyHostPath(hostPath);
        super.addFileSystemBind(hostPath, containerPath, mode);
    }

    @Override
    public void addFileSystemBind(String hostPath, String containerPath, BindMode mode, SelinuxContext selinuxContext)
    {
        verifyHostPath(hostPath);
        super.addFileSystemBind(hostPath, containerPath, mode, selinuxContext);
    }

    @Override
    public DockerContainer withFileSystemBind(String hostPath, String containerPath)
    {
        verifyHostPath(hostPath);
        return super.withFileSystemBind(hostPath, containerPath);
    }

    @Override
    public DockerContainer withFileSystemBind(String hostPath, String containerPath, BindMode mode)
    {
        verifyHostPath(hostPath);
        return super.withFileSystemBind(hostPath, containerPath, mode);
    }

    @Override
    public void copyFileToContainer(MountableFile mountableFile, String containerPath)
    {
        verifyHostPath(mountableFile.getResolvedPath());
        copyFileToContainer(containerPath, () -> super.copyFileToContainer(mountableFile, containerPath));
    }

    @Override
    public void copyFileToContainer(Transferable transferable, String containerPath)
    {
        copyFileToContainer(containerPath, () -> super.copyFileToContainer(transferable, containerPath));
    }

    private void copyFileToContainer(String containerPath, Runnable copy)
    {
        Stopwatch stopwatch = Stopwatch.createStarted();
        copy.run();
        log.info("Copied files into %s in %.1f s", containerPath, stopwatch.elapsed(MILLISECONDS) / 1000.);
    }

    // Mounting a non-existing file results in docker creating a directory. This is often not the desired effect. Fail fast instead.
    private static void verifyHostPath(String hostPath)
    {
        if (!Files.exists(Paths.get(hostPath))) {
            throw new IllegalArgumentException("Host path does not exist: " + hostPath);
        }
    }
}
