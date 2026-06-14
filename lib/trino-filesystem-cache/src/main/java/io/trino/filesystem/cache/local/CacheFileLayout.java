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
package io.trino.filesystem.cache.local;

import io.trino.filesystem.Location;

import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

final class CacheFileLayout
{
    static final String DATA_DIRECTORY = "data";
    static final String VERSION_DIRECTORY = "v1";

    private final Path root;

    CacheFileLayout(Path root)
    {
        this.root = requireNonNull(root, "root is null").toAbsolutePath().normalize();
    }

    CacheFile cacheFile(Location location, String cacheKey)
    {
        String fileHash = hash(cacheKey);
        Path fileGroupPath = locationPath(location)
                .resolve(fileHash)
                .normalize();
        if (!fileGroupPath.startsWith(root)) {
            throw new IllegalArgumentException("Cache path escapes root: " + fileGroupPath);
        }
        return new CacheFile(fileHash, fileGroupPath);
    }

    Path locationPath(Location location)
    {
        String locationHash = hash(location.toString());
        Path locationPath = root.resolve(VERSION_DIRECTORY)
                .resolve(DATA_DIRECTORY)
                .resolve(locationHash.substring(0, 2))
                .resolve(locationHash.substring(2, 4))
                .resolve(locationHash)
                .normalize();
        if (!locationPath.startsWith(root)) {
            throw new IllegalArgumentException("Cache path escapes root: " + locationPath);
        }
        return locationPath;
    }

    static String hash(String value)
    {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(UTF_8)));
        }
        catch (NoSuchAlgorithmException e) {
            throw new AssertionError("SHA-256 is not available", e);
        }
    }

    record CacheFile(String fileHash, Path fileGroupPath)
    {
        CacheFile
        {
            requireNonNull(fileHash, "fileHash is null");
            requireNonNull(fileGroupPath, "fileGroupPath is null");
        }

        Path pagePath(long pageIndex)
        {
            return fileGroupPath.resolve("%016x.cache".formatted(pageIndex));
        }

        Path temporaryPagePath(long pageIndex, String suffix)
        {
            return fileGroupPath.resolve("%016x.tmp.%s".formatted(pageIndex, suffix));
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("fileHash", fileHash)
                    .add("fileGroupPath", fileGroupPath)
                    .toString();
        }
    }
}
