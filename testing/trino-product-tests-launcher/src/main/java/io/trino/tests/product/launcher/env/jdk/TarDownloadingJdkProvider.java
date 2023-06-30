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
package io.trino.tests.product.launcher.env.jdk;

import com.github.dockerjava.api.model.AccessMode;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
import io.airlift.log.Logger;
import io.trino.testing.containers.TestContainers.DockerArchitecture;
import io.trino.testing.containers.TestContainers.DockerArchitectureInfo;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.EnvironmentOptions;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static io.trino.testing.containers.TestContainers.getDockerArchitectureInfo;
import static io.trino.tests.product.launcher.util.DirectoryUtils.getOnlyDescendant;
import static io.trino.tests.product.launcher.util.UriDownloader.download;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isDirectory;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public abstract class TarDownloadingJdkProvider
        implements JdkProvider
{
    private final Path downloadPath;
    private final Logger log = Logger.get(getClass());

    public TarDownloadingJdkProvider(EnvironmentOptions environmentOptions)
    {
        try {
            this.downloadPath = firstNonNull(environmentOptions.jdkDownloadPath, Files.createTempDirectory("ptl-temp-path"));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected abstract String getDownloadUri(DockerArchitecture architecture);

    @Override
    public String getJavaHome()
    {
        return "/usr/lib/jvm/" + getName();
    }

    @Override
    public DockerContainer applyTo(DockerContainer container)
    {
        ensureDownloadPathExists();
        String javaHome = getJavaHome();
        return container
                .withCreateContainerCmdModifier(cmd -> {
                    DockerArchitectureInfo architecture = getDockerArchitectureInfo(DockerImageName.parse(container.getDockerImageName()));
                    String downloadUri = getDownloadUri(architecture.imageArch());
                    String fullName = "JDK distribution '%s' for %s".formatted(getDescription(), architecture.imageArch());

                    verify(!isNullOrEmpty(downloadUri), "There is no download uri for " + fullName);
                    Path targetDownloadPath = downloadPath.resolve(getName() + "-" + architecture.imageArch().toString().toLowerCase(ENGLISH) + ".tar.gz");
                    Path extractPath = downloadPath.resolve(getName() + "-" + architecture.imageArch().toString().toLowerCase(ENGLISH));

                    synchronized (TarDownloadingJdkProvider.this) {
                        if (exists(targetDownloadPath)) {
                            log.info("%s already downloaded to %s", fullName, targetDownloadPath);
                        }
                        else if (!exists(extractPath)) { // Distribution not extracted and not downloaded yet
                            log.info("Downloading %s from %s to %s", fullName, downloadUri, targetDownloadPath);
                            download(downloadUri, targetDownloadPath, new EveryNthPercentProgress(progress -> log.info("Downloading %s %d%%...", fullName, progress), 5));
                            log.info("Downloaded %s to %s", fullName, targetDownloadPath);
                        }

                        if (exists(extractPath)) {
                            log.info("%s already extracted to %s", fullName, extractPath);
                        }
                        else {
                            extractTar(targetDownloadPath, extractPath);
                            try {
                                Files.deleteIfExists(targetDownloadPath);
                            }
                            catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }
                    }

                    Path javaHomePath = getOnlyDescendant(extractPath);
                    verify(exists(javaHomePath.resolve("bin/java")), "bin/java does not exist in %s", javaHomePath);

                    log.info("Mounting %s from %s in container '%s':%s", fullName, javaHomePath, container.getLogicalName(), javaHome);

                    Bind[] binds = cmd.getHostConfig().getBinds();
                    binds = Arrays.copyOf(binds, binds.length + 1);
                    binds[binds.length - 1] = new Bind(
                            javaHomePath.toAbsolutePath().toString(),
                            new Volume(javaHome),
                            AccessMode.ro);
                    cmd.getHostConfig().setBinds(binds);
                })
                .withEnv("JAVA_HOME", javaHome);
    }

    private static void extractTar(Path filePath, Path extractPath)
    {
        try {
            try (TarArchiveInputStream archiveStream = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(filePath.toFile())))) {
                TarArchiveEntry entry;
                while ((entry = archiveStream.getNextTarEntry()) != null) {
                    if (!archiveStream.canReadEntryData(entry)) {
                        continue;
                    }
                    if (entry.isDirectory()) {
                        continue;
                    }
                    File currentFile = extractPath.resolve(entry.getName()).toFile();
                    File parent = currentFile.getParentFile();
                    if (!parent.exists()) {
                        verify(parent.mkdirs(), "Could not create directory %s", parent);
                    }
                    try (OutputStream output = Files.newOutputStream(currentFile.toPath())) {
                        IOUtils.copy(archiveStream, output, 16 * 1024);

                        boolean isExecutable = (entry.getMode() & 0100) > 0;
                        if (isExecutable) {
                            verify(currentFile.setExecutable(true), "Could not set file %s as executable", currentFile.getAbsolutePath());
                        }
                    }
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void ensureDownloadPathExists()
    {
        if (!exists(downloadPath)) {
            try {
                Files.createDirectories(downloadPath);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        verify(isDirectory(downloadPath), "--jdk-tmp-download-path '%s' is not a directory", downloadPath);
    }

    private static class EveryNthPercentProgress
            implements Consumer<Integer>
    {
        private final AtomicInteger currentProgress = new AtomicInteger(0);
        private final Consumer<Integer> delegate;
        private final int n;

        public EveryNthPercentProgress(Consumer<Integer> delegate, int n)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.n = n;
        }

        @Override
        public void accept(Integer percent)
        {
            int currentBand = currentProgress.get() / n;
            int band = percent / n;

            if (band == currentBand) {
                return;
            }

            if (currentProgress.compareAndSet(currentBand * n, band * n)) {
                delegate.accept(percent);
            }
        }
    }
}
