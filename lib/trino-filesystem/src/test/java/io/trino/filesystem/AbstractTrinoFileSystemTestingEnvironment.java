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
package io.trino.filesystem;

import com.google.common.io.Closer;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTrinoFileSystemTestingEnvironment
{
    protected static final String TEST_BLOB_CONTENT_PREFIX = "test blob content for ";

    protected abstract boolean isHierarchical();

    public abstract TrinoFileSystem getFileSystem();

    protected abstract Location getRootLocation();

    protected abstract void verifyFileSystemIsEmpty();

    protected boolean supportsCreateWithoutOverwrite()
    {
        return true;
    }

    protected boolean supportsRenameFile()
    {
        return true;
    }

    protected boolean deleteFileFailsIfNotExists()
    {
        return true;
    }

    protected boolean normalizesListFilesResult()
    {
        return false;
    }

    protected boolean seekPastEndOfFileFails()
    {
        return true;
    }

    public static String getRequiredEnvironmentVariable(String name)
    {
        return requireNonNull(System.getenv(name), "Environment variable not set: " + name);
    }

    public Location createLocation(String path)
    {
        if (path.isEmpty()) {
            return getRootLocation();
        }
        return getRootLocation().appendPath(path);
    }

    public String readLocation(Location path)
    {
        return readLocation(path, getFileSystem());
    }

    public static String readLocation(Location path, TrinoFileSystem trinoFileSystem)
    {
        try (InputStream inputStream = trinoFileSystem.newInputFile(path).newStream()) {
            return new String(inputStream.readAllBytes(), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Location createBlob(Closer closer, String path)
    {
        Location location = createLocation(path);
        closer.register(new TempBlob(location, getFileSystem())).createOrOverwrite(TEST_BLOB_CONTENT_PREFIX + location.toString());
        return location;
    }

    public TempBlob randomBlobLocation(String nameHint)
    {
        TempBlob tempBlob = new TempBlob(createLocation("%s/%s".formatted(nameHint, UUID.randomUUID())), getFileSystem());
        assertThat(tempBlob.exists()).isFalse();
        return tempBlob;
    }
}
