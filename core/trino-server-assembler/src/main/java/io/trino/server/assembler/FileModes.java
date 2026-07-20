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
package io.trino.server.assembler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Set;

import static java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.GROUP_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_READ;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

/**
 * Translates between Unix file modes and {@link PosixFilePermission} sets.
 * <p>
 * The launcher ships native executables that must stay executable, so modes have
 * to survive the round trip from the source archive into the assembled tar.
 */
final class FileModes
{
    public static final int DEFAULT_FILE_MODE = 0644;
    public static final int DEFAULT_DIRECTORY_MODE = 0755;

    /**
     * Ordered from the most to the least significant bit of a mode.
     */
    private static final List<PosixFilePermission> PERMISSIONS = ImmutableList.of(
            OWNER_READ,
            OWNER_WRITE,
            OWNER_EXECUTE,
            GROUP_READ,
            GROUP_WRITE,
            GROUP_EXECUTE,
            OTHERS_READ,
            OTHERS_WRITE,
            OTHERS_EXECUTE);

    private static final boolean POSIX_SUPPORTED = FileSystems.getDefault().supportedFileAttributeViews().contains("posix");

    private FileModes() {}

    /**
     * Returns a usable mode for an archive entry. Archives written on systems
     * without Unix permissions report a mode of zero.
     */
    public static int normalize(int mode, boolean directory)
    {
        int permissions = mode & 0777;
        if (permissions != 0) {
            return permissions;
        }
        if (directory) {
            return DEFAULT_DIRECTORY_MODE;
        }
        return DEFAULT_FILE_MODE;
    }

    public static void apply(Path file, int mode)
            throws IOException
    {
        if (!POSIX_SUPPORTED) {
            return;
        }
        ImmutableSet.Builder<PosixFilePermission> permissions = ImmutableSet.builder();
        for (int i = 0; i < PERMISSIONS.size(); i++) {
            if ((mode & bit(i)) != 0) {
                permissions.add(PERMISSIONS.get(i));
            }
        }
        Files.setPosixFilePermissions(file, permissions.build());
    }

    public static int modeOf(Path file, boolean directory)
            throws IOException
    {
        if (!POSIX_SUPPORTED) {
            return normalize(0, directory);
        }
        Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(file);
        int mode = 0;
        for (int i = 0; i < PERMISSIONS.size(); i++) {
            if (permissions.contains(PERMISSIONS.get(i))) {
                mode |= bit(i);
            }
        }
        return mode;
    }

    private static int bit(int index)
    {
        return 1 << (PERMISSIONS.size() - 1 - index);
    }
}
