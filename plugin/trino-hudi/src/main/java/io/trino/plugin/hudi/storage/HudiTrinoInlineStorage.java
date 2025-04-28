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
package io.trino.plugin.hudi.storage;

import io.trino.filesystem.TrinoInputStream;
import io.trino.plugin.hudi.io.InlineSeekableDataInputStream;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.inline.InLineFSUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;

public class HudiTrinoInlineStorage
        extends HoodieStorage
{
    private final HudiTrinoStorage storage;

    public HudiTrinoInlineStorage(HudiTrinoStorage storage)
    {
        super(storage.getConf());
        this.storage = storage;
    }

    @Override
    public HoodieStorage newInstance(StoragePath path, StorageConfiguration<?> storageConf)
    {
        if (InLineFSUtils.SCHEME.equals(path.toUri().getScheme())) {
            return this;
        }
        return storage;
    }

    @Override
    public String getScheme()
    {
        return InLineFSUtils.SCHEME;
    }

    @Override
    public int getDefaultBlockSize(StoragePath path)
    {
        return storage.getDefaultBlockSize(getFilePathFromInlinePath(path));
    }

    @Override
    public int getDefaultBufferSize()
    {
        return storage.getDefaultBufferSize();
    }

    @Override
    public short getDefaultReplication(StoragePath path)
    {
        return storage.getDefaultReplication(getFilePathFromInlinePath(path));
    }

    @Override
    public URI getUri()
    {
        return URI.create(getScheme());
    }

    @Override
    public OutputStream create(StoragePath path, boolean b)
            throws IOException
    {
        throw new HoodieNotSupportedException("Hudi inline storage supports reads only.");
    }

    @Override
    public OutputStream create(StoragePath path, boolean b, Integer integer, Short aShort, Long aLong)
            throws IOException
    {
        throw new HoodieNotSupportedException("Hudi inline storage supports reads only.");
    }

    @Override
    public InputStream open(StoragePath path)
            throws IOException
    {
        return openSeekable(path, getDefaultBufferSize(), false);
    }

    @Override
    public SeekableDataInputStream openSeekable(StoragePath path, int bufferSize, boolean wrapStream)
            throws IOException
    {
        return new InlineSeekableDataInputStream(
                (TrinoInputStream) storage.open(getFilePathFromInlinePath(path)),
                InLineFSUtils.startOffset(path),
                InLineFSUtils.length(path));
    }

    @Override
    public OutputStream append(StoragePath path)
            throws IOException
    {
        throw new HoodieNotSupportedException("Hudi inline storage supports reads only.");
    }

    @Override
    public boolean exists(StoragePath path)
            throws IOException
    {
        return storage.exists(getFilePathFromInlinePath(path));
    }

    @Override
    public StoragePathInfo getPathInfo(StoragePath path)
            throws IOException
    {
        StoragePathInfo pathInfo = storage.getPathInfo(getFilePathFromInlinePath(path));
        return new StoragePathInfo(
                path, InLineFSUtils.length(path), pathInfo.isDirectory(),
                pathInfo.getBlockReplication(), pathInfo.getBlockSize(),
                pathInfo.getModificationTime());
    }

    @Override
    public boolean createDirectory(StoragePath path)
            throws IOException
    {
        throw new HoodieNotSupportedException("Hudi inline storage supports reads only.");
    }

    @Override
    public List<StoragePathInfo> listDirectEntries(StoragePath path)
            throws IOException
    {
        // This is supposed to be called on a file path only
        StoragePathInfo pathInfo = getPathInfo(path);
        ValidationUtils.checkArgument(pathInfo.isFile(),
                "HudiTrinoInlineStorage#listDirectEntries should only be called on a file path");
        return Collections.singletonList(pathInfo);
    }

    @Override
    public List<StoragePathInfo> listFiles(StoragePath path)
            throws IOException
    {
        throw new HoodieNotSupportedException("This API should not be called by Hudi inline storage.");
    }

    @Override
    public List<StoragePathInfo> listDirectEntries(StoragePath path, StoragePathFilter storagePathFilter)
            throws IOException
    {
        throw new HoodieNotSupportedException("This API should not be called by Hudi inline storage.");
    }

    @Override
    public List<StoragePathInfo> globEntries(StoragePath path, StoragePathFilter storagePathFilter)
            throws IOException
    {
        throw new HoodieNotSupportedException("This API should not be called by Hudi inline storage.");
    }

    @Override
    public boolean rename(StoragePath oldPath, StoragePath newPath)
            throws IOException
    {
        throw new HoodieNotSupportedException("Hudi inline storage supports reads only.");
    }

    @Override
    public boolean deleteDirectory(StoragePath path)
            throws IOException
    {
        throw new HoodieNotSupportedException("Hudi inline storage supports reads only.");
    }

    @Override
    public boolean deleteFile(StoragePath path)
            throws IOException
    {
        throw new HoodieNotSupportedException("Hudi inline storage supports reads only.");
    }

    @Override
    public void setModificationTime(StoragePath path, long modificationTime)
            throws IOException
    {
        throw new HoodieNotSupportedException("Hudi inline storage supports reads only.");
    }

    @Override
    public Object getFileSystem()
    {
        return storage.getFileSystem();
    }

    @Override
    public HoodieStorage getRawStorage()
    {
        return this;
    }

    @Override
    public void close()
            throws IOException
    {
    }

    private static StoragePath getFilePathFromInlinePath(StoragePath inlinePath)
    {
        assertInlineFSPath(inlinePath);
        String outerFileScheme = inlinePath.getParent().getName();
        StoragePath basePath = inlinePath.getParent().getParent();
        ValidationUtils.checkArgument(basePath.toString().contains(":"), "Invalid InLineFS path: " + inlinePath);
        String pathExceptScheme = basePath.toString().substring(basePath.toString().indexOf(":") + 1);
        String fullPath = outerFileScheme + ":" + (outerFileScheme.equals("file") || outerFileScheme.equals("local") ? "/" : "") + pathExceptScheme;
        return new StoragePath(fullPath);
    }

    private static void assertInlineFSPath(StoragePath inlinePath)
    {
        String scheme = inlinePath.toUri().getScheme();
        ValidationUtils.checkArgument(InLineFSUtils.SCHEME.equals(scheme));
    }
}
