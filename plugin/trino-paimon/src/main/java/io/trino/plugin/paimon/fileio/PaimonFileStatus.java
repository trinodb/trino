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

import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;

/**
 * File status for trino file.
 */
public class PaimonFileStatus
        implements FileStatus
{
    private final long len;
    private final Path path;
    private final long modificationTmie;

    public PaimonFileStatus(long len, Path path, long modificationTmie)
    {
        this.len = len;
        this.path = path;
        this.modificationTmie = modificationTmie;
    }

    @Override
    public long getLen()
    {
        return len;
    }

    @Override
    public boolean isDir()
    {
        return false;
    }

    @Override
    public Path getPath()
    {
        return path;
    }

    @Override
    public long getModificationTime()
    {
        return modificationTmie;
    }
}
