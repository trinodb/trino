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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

// TODO: Test with InterfaceTestUtils after merging: https://github.com/trinodb/trino/pull/3034
public class AccessTrackingFileSystem
        extends FileSystem
{
    private final FileSystem fileSystemDelegate;
    private Map<String, Integer> openedFiles = new HashMap<>();

    public AccessTrackingFileSystem(FileSystem fileSystem)
    {
        this.fileSystemDelegate = fileSystem;
    }

    @Override
    public URI getUri()
    {
        return fileSystemDelegate.getUri();
    }

    @Override
    public FSDataInputStream open(Path path, int i)
            throws IOException
    {
        incrementOpenCount(path.getName());
        return fileSystemDelegate.open(path, i);
    }

    @Override
    public FSDataInputStream open(Path f)
            throws IOException
    {
        incrementOpenCount(f.getName());
        return fileSystemDelegate.open(f);
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l, Progressable progressable)
            throws IOException
    {
        return fileSystemDelegate.create(path, fsPermission, b, i, i1, l, progressable);
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable)
            throws IOException
    {
        return fileSystemDelegate.append(path, i, progressable);
    }

    @Override
    public boolean rename(Path path, Path path1)
            throws IOException
    {
        return fileSystemDelegate.rename(path, path1);
    }

    @Override
    public boolean delete(Path path, boolean b)
            throws IOException
    {
        return fileSystemDelegate.delete(path, b);
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException
    {
        return fileSystemDelegate.listStatus(path);
    }

    @Override
    public void setWorkingDirectory(Path path)
    {
        fileSystemDelegate.setWorkingDirectory(path);
    }

    @Override
    public Path getWorkingDirectory()
    {
        return fileSystemDelegate.getWorkingDirectory();
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission)
            throws IOException
    {
        return fileSystemDelegate.mkdirs(path, fsPermission);
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        return fileSystemDelegate.getFileStatus(path);
    }

    public Map<String, Integer> getOpenCount()
    {
        return ImmutableMap.copyOf(openedFiles);
    }

    private void incrementOpenCount(String fileName)
    {
        openedFiles.put(fileName, openedFiles.getOrDefault(fileName, 0) + 1);
    }
}
