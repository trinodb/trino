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
package io.trino.filesystem.alluxio;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ListStatusPOptions;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.filesystem.alluxio.AlluxioUtils.convertToAlluxioURI;
import static io.trino.filesystem.alluxio.AlluxioUtils.getAlluxioBase;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class AlluxioFileSystem
        implements TrinoFileSystem
{
    private final FileSystem alluxioClient;
    private final String mountRoot;
    private final Location rootLocation;

    public AlluxioFileSystem(FileSystem alluxioClient)
    {
        this(alluxioClient, "/", Location.of("alluxio:///"));
    }

    public AlluxioFileSystem(FileSystem alluxioClient, String mountRoot, Location rootLocation)
    {
        this.alluxioClient = requireNonNull(alluxioClient, "filesystem is null");
        this.mountRoot = mountRoot; // default alluxio mount root
        this.rootLocation = requireNonNull(rootLocation, "rootLocation is null");
    }

    public String getMountRoot()
    {
        return mountRoot;
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        ensureNotRootLocation(location);
        ensureNotEndWithSlash(location);
        return new AlluxioFileSystemInputFile(location, null, alluxioClient, mountRoot, Optional.empty());
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        ensureNotRootLocation(location);
        ensureNotEndWithSlash(location);
        return new AlluxioFileSystemInputFile(location, length, alluxioClient, mountRoot, Optional.empty());
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        ensureNotRootLocation(location);
        ensureNotEndWithSlash(location);
        return new AlluxioFileSystemInputFile(location, length, alluxioClient, mountRoot, Optional.of(lastModified));
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        ensureNotRootLocation(location);
        ensureNotEndWithSlash(location);
        return new AlluxioFileSystemOutputFile(rootLocation, location, alluxioClient, mountRoot);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        ensureNotRootLocation(location);
        ensureNotEndWithSlash(location);
        try {
            alluxioClient.delete(convertToAlluxioURI(location, mountRoot));
        }
        catch (FileDoesNotExistException _) {
        }
        catch (AlluxioException e) {
            throw new IOException("Error deleteFile %s".formatted(location), e);
        }
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        try {
            AlluxioURI uri = convertToAlluxioURI(location, mountRoot);
            URIStatus status = alluxioClient.getStatus(uri);
            if (status == null) {
                return;
            }
            if (!status.isFolder()) {
                throw new IOException("delete directory cannot be called on a file %s".formatted(location));
            }
            DeletePOptions deletePOptions = DeletePOptions.newBuilder().setRecursive(true).build();
            // recursive delete on the root directory must be handled manually
            if (location.path().isEmpty() || location.path().equals(mountRoot)) {
                for (URIStatus uriStatus : alluxioClient.listStatus(uri)) {
                    alluxioClient.delete(new AlluxioURI(uriStatus.getPath()), deletePOptions);
                }
            }
            else {
                alluxioClient.delete(uri, deletePOptions);
            }
        }
        catch (FileDoesNotExistException | NotFoundRuntimeException e) {
        }
        catch (AlluxioException e) {
            throw new IOException("Error deleteDirectory %s".formatted(location), e);
        }
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        try {
            ensureNotRootLocation(source);
            ensureNotEndWithSlash(source);
            ensureNotRootLocation(target);
            ensureNotEndWithSlash(target);
        }
        catch (IllegalStateException e) {
            throw new IllegalStateException(
                    "Cannot rename file from %s to %s as one of them is root location".formatted(source, target), e);
        }
        AlluxioURI sourceUri = convertToAlluxioURI(source, mountRoot);
        AlluxioURI targetUri = convertToAlluxioURI(target, mountRoot);

        try {
            if (!alluxioClient.exists(sourceUri)) {
                throw new IOException(
                        "Cannot rename file %s to %s as file %s doesn't exist".formatted(source, target, source));
            }
            if (alluxioClient.exists(targetUri)) {
                throw new IOException(
                        "Cannot rename file %s to %s as file %s already exists".formatted(source, target, target));
            }
            URIStatus status = alluxioClient.getStatus(sourceUri);
            if (status.isFolder()) {
                throw new IOException(
                        "Cannot rename file %s to %s as %s is a directory".formatted(source, target, source));
            }
            alluxioClient.rename(convertToAlluxioURI(source, mountRoot), convertToAlluxioURI(target, mountRoot));
        }
        catch (AlluxioException e) {
            throw new IOException("Error renameFile from %s to %s".formatted(source, target), e);
        }
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        try {
            URIStatus status = alluxioClient.getStatus(convertToAlluxioURI(location, mountRoot));
            if (status == null) {
                new AlluxioFileIterator(Collections.emptyList(), getAlluxioBase(location.toString()));
            }
            if (!status.isFolder()) {
                throw new IOException("Location is not a directory: %s".formatted(location));
            }
        }
        catch (NotFoundRuntimeException | AlluxioException e) {
            return new AlluxioFileIterator(Collections.emptyList(), getAlluxioBase(location.toString()));
        }

        try {
            List<URIStatus> filesStatus = alluxioClient.listStatus(convertToAlluxioURI(location, mountRoot),
                    ListStatusPOptions.newBuilder().setRecursive(true).build());
            return new AlluxioFileIterator(filesStatus.stream().filter(status -> !status.isFolder() & status.isCompleted()).toList(), getAlluxioBase(location.toString()));
        }
        catch (AlluxioException e) {
            throw new IOException("Error listFiles %s".formatted(location), e);
        }
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        if (location.path().isEmpty()) {
            return Optional.of(true);
        }
        try {
            URIStatus status = alluxioClient.getStatus(convertToAlluxioURI(location, mountRoot));
            if (status != null && status.isFolder()) {
                return Optional.of(true);
            }
            return Optional.of(false);
        }
        catch (FileDoesNotExistException | FileNotFoundException | NotFoundRuntimeException e) {
            return Optional.of(false);
        }
        catch (AlluxioException e) {
            throw new IOException("Error directoryExists %s".formatted(location), e);
        }
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        try {
            AlluxioURI locationUri = convertToAlluxioURI(location, mountRoot);
            if (alluxioClient.exists(locationUri)) {
                URIStatus status = alluxioClient.getStatus(locationUri);
                if (!status.isFolder()) {
                    throw new IOException(
                            "Cannot create a directory for an existing file location %s".formatted(location));
                }
            }
            alluxioClient.createDirectory(
                    locationUri,
                    CreateDirectoryPOptions.newBuilder()
                            .setAllowExists(true)
                            .setRecursive(true)
                            .build());
        }
        catch (AlluxioException e) {
            throw new IOException("Error createDirectory %s".formatted(location), e);
        }
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        try {
            ensureNotRootLocation(source);
            ensureNotRootLocation(target);
        }
        catch (IllegalStateException e) {
            throw new IOException(
                    "Cannot rename directory from %s to %s as one of them is root location".formatted(source, target), e);
        }
        try {
            if (alluxioClient.exists(convertToAlluxioURI(target, mountRoot))) {
                throw new IOException(
                        "Cannot rename %s to %s as file %s already exists".formatted(source, target, target));
            }
            alluxioClient.rename(convertToAlluxioURI(source, mountRoot), convertToAlluxioURI(target, mountRoot));
        }
        catch (AlluxioException e) {
            throw new IOException("Error renameDirectory from %s to %s".formatted(source, target), e);
        }
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        try {
            if (isFile(location)) {
                throw new IOException("Cannot list directories for a file %s".formatted(location));
            }
            List<URIStatus> filesStatus = alluxioClient.listStatus(convertToAlluxioURI(location, mountRoot));
            return filesStatus.stream()
                    .filter(URIStatus::isFolder)
                    .map((URIStatus fileStatus) -> AlluxioUtils.convertToLocation(fileStatus.getPath(), mountRoot))
                    .map(loc -> {
                        if (!loc.toString().endsWith("/")) {
                            return Location.of(loc + "/");
                        }
                        else {
                            return loc;
                        }
                    })
                    .collect(Collectors.toSet());
        }
        catch (FileDoesNotExistException | FileNotFoundException | NotFoundRuntimeException e) {
            return Collections.emptySet();
        }
        catch (AlluxioException e) {
            throw new IOException("Error listDirectories %s".formatted(location), e);
        }
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        // allow for absolute or relative temporary prefix
        Location temporary;
        if (temporaryPrefix.startsWith("/")) {
            String prefix = temporaryPrefix;
            while (prefix.startsWith("/")) {
                prefix = prefix.substring(1);
            }
            temporary = targetPath.appendPath(prefix);
        }
        else {
            temporary = targetPath.appendPath(temporaryPrefix);
        }

        temporary = temporary.appendPath(randomUUID().toString());

        createDirectory(temporary);
        return Optional.of(temporary);
    }

    private void ensureNotRootLocation(Location location)
    {
        String locationPath = location.path();
        while (locationPath.endsWith("/")) {
            locationPath = locationPath.substring(0, locationPath.length() - 1);
        }

        String rootLocationPath = rootLocation.path();
        while (rootLocationPath.endsWith("/")) {
            rootLocationPath = rootLocationPath.substring(0, rootLocationPath.length() - 1);
        }

        if (rootLocationPath.equals(locationPath)) {
            throw new IllegalStateException("Illegal operation on %s".formatted(location));
        }
    }

    private void ensureNotEndWithSlash(Location location)
    {
        String locationPath = location.path();
        if (locationPath.endsWith("/")) {
            throw new IllegalStateException("Illegal operation on %s".formatted(location));
        }
    }

    private boolean isFile(Location location)
    {
        try {
            URIStatus status = alluxioClient.getStatus(convertToAlluxioURI(location, mountRoot));
            if (status == null) {
                return false;
            }
            return !status.isFolder();
        }
        catch (NotFoundRuntimeException | AlluxioException | IOException e) {
            return false;
        }
    }
}
