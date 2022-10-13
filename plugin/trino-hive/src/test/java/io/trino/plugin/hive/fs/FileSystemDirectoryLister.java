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
package io.trino.plugin.hive.fs;

import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;

public class FileSystemDirectoryLister
        implements DirectoryLister
{
    @Override
    public RemoteIterator<TrinoFileStatus> list(FileSystem fs, Table table, Path path)
            throws IOException
    {
        return new TrinoFileStatusRemoteIterator(fs.listLocatedStatus(path));
    }

    @Override
    public RemoteIterator<TrinoFileStatus> listFilesRecursively(FileSystem fs, Table table, Path path)
            throws IOException
    {
        return new TrinoFileStatusRemoteIterator(fs.listFiles(path, true));
    }

    @Override
    public void invalidate(Partition partition)
    {
    }

    @Override
    public void invalidate(Table table)
    {
    }
}
