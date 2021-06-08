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
package io.trino.tests.product.launcher.docker;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.ClassPath;
import io.airlift.log.Logger;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static java.nio.file.Files.copy;
import static java.util.UUID.randomUUID;

public final class DockerFiles
        implements AutoCloseable
{
    public static final String ROOT_PATH = "docker/presto-product-tests/";

    private static final Logger log = Logger.get(DockerFiles.class);

    @GuardedBy("this")
    private Path dockerFilesHostPath;
    @GuardedBy("this")
    private boolean closed;

    @PreDestroy
    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        if (dockerFilesHostPath != null) {
            Failsafe.with(new RetryPolicy<>().withMaxAttempts(5))
                    .run(() -> deleteRecursively(dockerFilesHostPath, ALLOW_INSECURE));
            dockerFilesHostPath = null;
        }
        closed = true;
    }

    public synchronized Path getDockerFilesHostPath()
    {
        checkState(!closed, "Already closed");
        if (dockerFilesHostPath == null) {
            dockerFilesHostPath = unpackDockerFilesFromClasspath();
            verify(dockerFilesHostPath != null);
        }
        return dockerFilesHostPath;
    }

    public ResourceProvider getDockerFilesHostDirectory(String... searchDirectories)
    {
        // Resources are searched for from last to the first provided search directory.
        List<Path> searchPaths = ImmutableList.copyOf(searchDirectories).reverse().stream()
                .map(this::getDockerFilesHostPath)
                .collect(toImmutableList());

        return new ResourceProvider()
        {
            @Override
            public Path getPath(String file)
            {
                checkArgument(file != null && !file.isEmpty() && !(file.charAt(0) == '/'), "Invalid file: %s", file);

                for (Path searchPath : searchPaths) {
                    Path filePath = searchPath.resolve(file);
                    if (Files.exists(filePath)) {
                        return filePath;
                    }
                }

                throw new IllegalArgumentException(format("Could not find '%s' in following directories: %s", file, Joiner.on(", ").join(searchPaths)));
            }

            @Override
            public Path getHostPath()
            {
                return getDockerFilesHostPath();
            }
        };
    }

    public Path getDockerFilesHostPath(String file)
    {
        checkArgument(file != null && !file.isEmpty() && !(file.charAt(0) == '/'), "Invalid file: %s", file);
        Path filePath = getDockerFilesHostPath().resolve(file);
        checkArgument(Files.exists(filePath), "'%s' resolves to '%s', but it does not exist", file, filePath);
        return filePath;
    }

    private static Path unpackDockerFilesFromClasspath()
    {
        try {
            Path dockerFilesHostPath = createTemporaryDirectoryForDocker();
            ClassPath.from(Thread.currentThread().getContextClassLoader())
                    .getResources().stream()
                    .filter(resourceInfo -> resourceInfo.getResourceName().startsWith(ROOT_PATH))
                    .forEach(resourceInfo -> {
                        try {
                            Path target = dockerFilesHostPath.resolve(resourceInfo.getResourceName().replaceFirst("^" + ROOT_PATH, ""));
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
    {
        Path temporaryDirectoryForDocker;
        try {
            // Cannot use Files.createTempDirectory() because on Mac by default it uses /var/folders/ which is not visible to Docker for Mac
            temporaryDirectoryForDocker = Files.createDirectory(Paths.get("/tmp/docker-files-" + randomUUID().toString()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

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

    public interface ResourceProvider
    {
        Path getPath(String resourceName);

        Path getHostPath();
    }
}
