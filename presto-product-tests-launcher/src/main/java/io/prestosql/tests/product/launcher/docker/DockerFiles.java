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
package io.prestosql.tests.product.launcher.docker;

import com.google.common.reflect.ClassPath;
import io.airlift.log.Logger;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.file.Files.copy;
import static java.util.UUID.randomUUID;

public final class DockerFiles
        implements AutoCloseable
{
    private static final Logger log = Logger.get(DockerFiles.class);

    @GuardedBy("this")
    private Path dockerFilesHostPath;
    @GuardedBy("this")
    private boolean closed;

    @PreDestroy
    @Override
    public synchronized void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        if (dockerFilesHostPath != null) {
            deleteRecursively(dockerFilesHostPath, ALLOW_INSECURE);
            dockerFilesHostPath = null;
        }
        closed = true;
    }

    public synchronized String getDockerFilesHostPath()
    {
        checkState(!closed, "Already closed");
        if (dockerFilesHostPath == null) {
            dockerFilesHostPath = unpackDockerFilesFromClasspath();
            verify(dockerFilesHostPath != null);
        }
        return dockerFilesHostPath.toString();
    }

    public String getDockerFilesHostPath(String file)
    {
        checkArgument(file != null && !file.isEmpty() && !file.startsWith("/"), "Invalid file: %s", file);
        Path filePath = Paths.get(getDockerFilesHostPath()).resolve(file);
        checkArgument(Files.exists(filePath), "'%s' resolves to '%s', but it does not exist", file, filePath);
        return filePath.toString();
    }

    private static Path unpackDockerFilesFromClasspath()
    {
        try {
            Path dockerFilesHostPath = createTemporaryDirectoryForDocker();
            ClassPath.from(Thread.currentThread().getContextClassLoader())
                    .getResources().stream()
                    .filter(resourceInfo -> resourceInfo.getResourceName().startsWith("docker/presto-product-tests/"))
                    .forEach(resourceInfo -> {
                        try {
                            Path target = dockerFilesHostPath.resolve(resourceInfo.getResourceName().replaceFirst("^docker/presto-product-tests/", ""));
                            Files.createDirectories(target.getParent());

                            try (InputStream inputStream = resourceInfo.asByteSource().openStream()) {
                                copy(inputStream, target);
                            }
                            if (resourceInfo.getResourceName().endsWith(".sh")) {
                                Files.setPosixFilePermissions(target, PosixFilePermissions.fromString("r-x------"));
                            }
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
            return dockerFilesHostPath;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Path createTemporaryDirectoryForDocker()
            throws IOException
    {
        // Cannot use Files.createTempDirectory() because on Mac by default it uses /var/folders/ which is not visible to Docker for Mac
        Path temporaryDirectoryForDocker = Files.createDirectory(Paths.get("/tmp/docker-files-" + randomUUID().toString()));

        // Best-effort cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                deleteRecursively(temporaryDirectoryForDocker, ALLOW_INSECURE);
            }
            catch (IOException e) {
                log.warn(e, "Failed to clean up docker files temporary directory '%s'", temporaryDirectoryForDocker);
            }
        }));
        return temporaryDirectoryForDocker;
    }
}
