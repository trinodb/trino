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
package io.trino.filesystem.local;

import io.trino.filesystem.Location;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.util.Objects.requireNonNull;

/**
 * Maps between {@link Location} paths and real filesystem paths for the {@code local}/{@code file}
 * schemes, and enforces that the result stays under the configured root.
 * <p>
 * {@code local://} paths are a virtual namespace, resolved relative to the root. {@code file://}
 * paths name the literal absolute path (matching Hadoop's {@code file:} scheme and RFC 8089). If a
 * legacy prefix is configured, a {@code file://} path starting with it is rebased under the root
 * instead — this supports migrating tables whose stored locations still reference a mount point
 * that has since moved; a path that doesn't match the legacy prefix is left as a literal absolute
 * path, unchanged.
 */
final class LocalPathCanonicalizer
{
    private final Path rootPath;
    private final Path canonicalRootPath;
    private final Optional<Path> legacyPrefix;

    LocalPathCanonicalizer(Path rootPath, Optional<Path> legacyPrefix)
    {
        this.rootPath = requireNonNull(rootPath, "rootPath is null");
        this.legacyPrefix = requireNonNull(legacyPrefix, "legacyPrefix is null");
        try {
            this.canonicalRootPath = rootPath.toRealPath();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    Path rootPath()
    {
        return rootPath;
    }

    /**
     * Returns whether {@code path} falls under the root. The lexical prefix check alone does not
     * account for a symlink under root pointing outside of it, so the nearest existing ancestor is
     * resolved to its canonical form to confirm it doesn't escape the canonical root.
     */
    boolean isWithinRoot(Path path)
    {
        if (!path.startsWith(rootPath)) {
            return false;
        }
        Path ancestor = path;
        while (!Files.exists(ancestor, NOFOLLOW_LINKS)) {
            ancestor = ancestor.getParent();
            if (ancestor == null) {
                return true;
            }
        }
        try {
            return ancestor.toRealPath().startsWith(canonicalRootPath);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    Path toRealPath(String scheme, String locationPath)
    {
        if (scheme.equals("file")) {
            Path absolutePath = Path.of("/", locationPath).normalize();
            return legacyPrefix
                    .filter(absolutePath::startsWith)
                    .map(prefix -> rootPath.resolve(prefix.relativize(absolutePath)).normalize())
                    .orElse(absolutePath);
        }
        return rootPath.resolve(locationPath).normalize();
    }

    Location toLocation(String scheme, Path path)
    {
        String locationPath = scheme.equals("file") ? path.toString().substring(1) : rootPath.relativize(path).toString();
        return Location.of(scheme + ":///" + locationPath);
    }
}
