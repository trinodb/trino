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
package io.trino.plugin.hudi.testing;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.google.common.io.Resources.getResource;
import static java.util.Objects.requireNonNull;

public class HudiTableUnzipper
{
    private static final String ZIP_EXT = ".zip";

    private HudiTableUnzipper() {}

    public static void unzipAllItemsInResource(String resourceName, Path outputPath)
            throws IOException, URISyntaxException
    {
        requireNonNull(resourceName, "Resource name cannot be null or empty.");

        URL resourceUrl = HudiTableUnzipper.class.getClassLoader().getResource(resourceName);
        if (resourceUrl == null) {
            throw new IOException("Resource not found: " + resourceName);
        }

        for (File file : Path.of(getResource(resourceName).toURI()).toFile().listFiles()) {
            if (file.isFile() && file.getName().endsWith(ZIP_EXT)) {
                // Only handle zip files
                unzipFile(file.toURI().toURL(), outputPath);
            }
        }
    }

    private static void unzipFile(URL resourceUrl, Path targetDirectory)
            throws IOException
    {
        try (InputStream is = resourceUrl.openStream(); ZipInputStream zis = new ZipInputStream(is)) {
            ZipEntry zipEntry = zis.getNextEntry();
            byte[] buffer = new byte[1024];

            while (zipEntry != null) {
                Path newFilePath = targetDirectory.resolve(zipEntry.getName());

                // Prevent Zip Slip vulnerability (Do not want files written outside the our target dir)
                if (!newFilePath.normalize().startsWith(targetDirectory.normalize())) {
                    throw new IOException("Bad zip entry: " + zipEntry.getName());
                }

                if (zipEntry.isDirectory()) {
                    // Handle directories
                    if (!Files.exists(newFilePath)) {
                        Files.createDirectories(newFilePath);
                    }
                }
                else {
                    // Ensure parent directory exists before handling files
                    Path parentDir = newFilePath.getParent();
                    if (parentDir != null && !Files.exists(parentDir)) {
                        Files.createDirectories(parentDir);
                    }

                    try (FileOutputStream fos = new FileOutputStream(newFilePath.toFile())) {
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }
                }
                zis.closeEntry();
                zipEntry = zis.getNextEntry();
            }
        }
    }

    public static void deleteInflatedFiles(Path path)
            throws URISyntaxException, IOException
    {
        requireNonNull(path, "path cannot be null or empty.");
        for (File file : path.toFile().listFiles()) {
            // Ignore all zip files
            if (file.isFile() && file.getName().endsWith(ZIP_EXT)) {
                continue;
            }
            // Not really required, as we are in the test-classes directory
            // Ensure that we are only deleting deflated folders of zip
            if (path.resolve(file.getName() + ZIP_EXT).toFile().exists()) {
                deleteFilesInDirectory(file.toPath());
            }
        }
    }

    private static void deleteFilesInDirectory(Path pathToDelete)
            throws IOException
    {
        // Recursively delete all files in path
        try (Stream<Path> filesInDir = Files.walk(pathToDelete)) {
            // Reverse order to delete all children before parent
            filesInDir.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }
}
