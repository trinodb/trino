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
package io.trino.plugin.paimon.fileio;

import io.trino.filesystem.TrinoFileSystem;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;

import static java.util.Objects.requireNonNull;

public class PaimonFileIOLoader
        implements FileIOLoader
{
    private final TrinoFileSystem trinoFileSystem;

    public PaimonFileIOLoader(TrinoFileSystem trinoFileSystem)
    {
        this.trinoFileSystem = requireNonNull(trinoFileSystem, "trinoFileSystem is null");
    }

    @Override
    public String getScheme()
    {
        return "trino";
    }

    @Override
    public FileIO load(Path path)
    {
        return new PaimonFileIO(trinoFileSystem, path);
    }
}
