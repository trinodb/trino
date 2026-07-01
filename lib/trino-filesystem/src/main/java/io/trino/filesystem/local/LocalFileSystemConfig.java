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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.AssertTrue;

import java.nio.file.Path;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.base.StandardSystemProperty.JAVA_IO_TMPDIR;

public class LocalFileSystemConfig
{
    // Path.of() collapses repeated slashes, so a value like "local:///storage/datalake" ends up
    // as "local:/storage/datalake" here; detect a URI scheme prefix rather than looking for "://".
    private static final Pattern URI_SCHEME_PREFIX = Pattern.compile("^[A-Za-z][A-Za-z0-9+.-]+:.*");

    private Path location = Path.of(System.getProperty(JAVA_IO_TMPDIR.key()));
    private Optional<Path> legacyPrefix = Optional.empty();

    public @FileExists Path getLocation()
    {
        return location;
    }

    @ConfigDescription("Local file system root directory")
    @Config("local.location")
    public LocalFileSystemConfig setLocation(Path location)
    {
        this.location = location;
        return this;
    }

    @AssertTrue(message = "local.location must be a plain filesystem path (e.g. /storage/datalake), not a URI")
    public boolean isLocationValid()
    {
        return !URI_SCHEME_PREFIX.matcher(location.toString()).matches();
    }

    public Optional<Path> getLegacyPrefix()
    {
        return legacyPrefix;
    }

    @ConfigDescription("Absolute path prefix to strip from file:// locations before resolving them under local.location; " +
            "for migrating tables whose stored locations still reference a previous mount point")
    @Config("local.legacy-prefix")
    public LocalFileSystemConfig setLegacyPrefix(Path legacyPrefix)
    {
        this.legacyPrefix = Optional.ofNullable(legacyPrefix);
        return this;
    }
}
