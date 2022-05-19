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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.DelegationTokenIssuer;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public void initialize(URI name, Configuration conf)
            throws IOException
    {
        fileSystemDelegate.initialize(name, conf);
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
    public FSDataInputStream open(PathHandle fd)
            throws IOException
    {
        // Retrieve file name and call incrementOpenCount if this method is required
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataInputStream open(PathHandle fd, int bufferSize)
            throws IOException
    {
        // Retrieve file name and call incrementOpenCount if this method is required
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus[] listStatus(Path[] files)
            throws FileNotFoundException, IOException
    {
        return fileSystemDelegate.listStatus(files);
    }

    @Override
    public FileStatus[] listStatus(Path f, PathFilter filter)
            throws FileNotFoundException, IOException
    {
        return fileSystemDelegate.listStatus(f, filter);
    }

    @Override
    public FileStatus[] listStatus(Path[] files, PathFilter filter)
            throws FileNotFoundException, IOException
    {
        return fileSystemDelegate.listStatus(files, filter);
    }

    @Override
    public FsStatus getStatus()
            throws IOException
    {
        return fileSystemDelegate.getStatus();
    }

    @Override
    public FsStatus getStatus(Path f)
            throws IOException
    {
        incrementOpenCount(f.getName());
        return fileSystemDelegate.getStatus(f);
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern)
            throws IOException
    {
        return fileSystemDelegate.globStatus(pathPattern);
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
            throws IOException
    {
        return fileSystemDelegate.globStatus(pathPattern, filter);
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l, Progressable progressable)
            throws IOException
    {
        return fileSystemDelegate.create(path, fsPermission, b, i, i1, l, progressable);
    }

    @Override
    public FSDataOutputStream create(Path f)
            throws IOException
    {
        return fileSystemDelegate.create(f);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite)
            throws IOException
    {
        return fileSystemDelegate.create(f, overwrite);
    }

    @Override
    public FSDataOutputStream create(Path f, Progressable progress)
            throws IOException
    {
        return fileSystemDelegate.create(f, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, short replication)
            throws IOException
    {
        return fileSystemDelegate.create(f, replication);
    }

    @Override
    public FSDataOutputStream create(Path f, short replication, Progressable progress)
            throws IOException
    {
        return fileSystemDelegate.create(f, replication, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize)
            throws IOException
    {
        return fileSystemDelegate.create(f, overwrite, bufferSize);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress)
            throws IOException
    {
        return fileSystemDelegate.create(f, overwrite, bufferSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
            throws IOException
    {
        return fileSystemDelegate.create(f, overwrite, bufferSize, replication, blockSize);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        return fileSystemDelegate.create(f, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        return fileSystemDelegate.create(f, permission, flags, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt)
            throws IOException
    {
        return fileSystemDelegate.create(f, permission, flags, bufferSize, replication, blockSize, progress, checksumOpt);
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        return fileSystemDelegate.createNonRecursive(f, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        return fileSystemDelegate.createNonRecursive(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        return fileSystemDelegate.createNonRecursive(f, permission, flags, bufferSize, replication, blockSize, progress);
    }

    @Override
    public boolean createNewFile(Path f)
            throws IOException
    {
        return fileSystemDelegate.createNewFile(f);
    }

    @Override
    public FSDataOutputStreamBuilder appendFile(Path path)
    {
        return fileSystemDelegate.appendFile(path);
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable)
            throws IOException
    {
        return fileSystemDelegate.append(path, i, progressable);
    }

    @Override
    public FSDataOutputStream append(Path f)
            throws IOException
    {
        return fileSystemDelegate.append(f);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize)
            throws IOException
    {
        return fileSystemDelegate.append(f, bufferSize);
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
    public boolean deleteOnExit(Path f)
            throws IOException
    {
        return fileSystemDelegate.deleteOnExit(f);
    }

    @Override
    public boolean cancelDeleteOnExit(Path f)
    {
        return fileSystemDelegate.cancelDeleteOnExit(f);
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
    public String getCanonicalServiceName()
    {
        return fileSystemDelegate.getCanonicalServiceName();
    }

    @Override
    public FileSystem[] getChildFileSystems()
    {
        return fileSystemDelegate.getChildFileSystems();
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission)
            throws IOException
    {
        return fileSystemDelegate.mkdirs(path, fsPermission);
    }

    @Override
    public Path makeQualified(Path path)
    {
        return fileSystemDelegate.makeQualified(path);
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        return fileSystemDelegate.getFileStatus(path);
    }

    @Override
    public long getUsed()
            throws IOException
    {
        return fileSystemDelegate.getUsed();
    }

    @Override
    public long getUsed(Path path)
            throws IOException
    {
        return fileSystemDelegate.getUsed(path);
    }

    @Override
    public Token<?> getDelegationToken(String renewer)
            throws IOException
    {
        return fileSystemDelegate.getDelegationToken(renewer);
    }

    @Override
    public DelegationTokenIssuer[] getAdditionalTokenIssuers()
            throws IOException
    {
        return fileSystemDelegate.getAdditionalTokenIssuers();
    }

    @Override
    public Token<?>[] addDelegationTokens(String renewer, Credentials credentials)
            throws IOException
    {
        return fileSystemDelegate.addDelegationTokens(renewer, credentials);
    }

    @Override
    public void setAcl(Path path, List<AclEntry> aclSpec)
            throws IOException
    {
        fileSystemDelegate.setAcl(path, aclSpec);
    }

    @Override
    public void removeAcl(Path path)
            throws IOException
    {
        fileSystemDelegate.removeAcl(path);
    }

    @Override
    public List<String> listXAttrs(Path path)
            throws IOException
    {
        return fileSystemDelegate.listXAttrs(path);
    }

    @Override
    public byte[] getXAttr(Path path, String name)
            throws IOException
    {
        return fileSystemDelegate.getXAttr(path, name);
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path)
            throws IOException
    {
        return fileSystemDelegate.getXAttrs(path);
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path, List<String> names)
            throws IOException
    {
        return fileSystemDelegate.getXAttrs(path, names);
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value)
            throws IOException
    {
        fileSystemDelegate.setXAttr(path, name, value);
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flag)
            throws IOException
    {
        fileSystemDelegate.setXAttr(path, name, value, flag);
    }

    @Override
    public void removeXAttr(Path path, String name)
            throws IOException
    {
        fileSystemDelegate.removeXAttr(path, name);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException
    {
        return fileSystemDelegate.getFileBlockLocations(file, start, len);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path p, long start, long len)
            throws IOException
    {
        return fileSystemDelegate.getFileBlockLocations(p, start, len);
    }

    @Override
    public QuotaUsage getQuotaUsage(Path f)
            throws IOException
    {
        return fileSystemDelegate.getQuotaUsage(f);
    }

    @Override
    public FsServerDefaults getServerDefaults()
            throws IOException
    {
        return fileSystemDelegate.getServerDefaults();
    }

    @Override
    public long getDefaultBlockSize()
    {
        return fileSystemDelegate.getDefaultBlockSize();
    }

    @Override
    public long getDefaultBlockSize(Path f)
    {
        return fileSystemDelegate.getDefaultBlockSize(f);
    }

    @Override
    public FsServerDefaults getServerDefaults(Path p)
            throws IOException
    {
        return fileSystemDelegate.getServerDefaults(p);
    }

    @Override
    public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
            throws IOException
    {
        fileSystemDelegate.completeLocalOutput(fsOutputFile, tmpLocalFile);
    }

    @Override
    public String getScheme()
    {
        return fileSystemDelegate.getScheme();
    }

    @Override
    public String getName()
    {
        return fileSystemDelegate.getName();
    }

    @Override
    public Path resolvePath(Path p)
            throws IOException
    {
        return fileSystemDelegate.resolvePath(p);
    }

    @Override
    public void concat(Path trg, Path[] psrcs)
            throws IOException
    {
        fileSystemDelegate.concat(trg, psrcs);
    }

    @Override
    public short getReplication(Path src)
            throws IOException
    {
        return fileSystemDelegate.getReplication(src);
    }

    @Override
    public boolean setReplication(Path src, short replication)
            throws IOException
    {
        return fileSystemDelegate.setReplication(src, replication);
    }

    @Override
    public boolean truncate(Path f, long newLength)
            throws IOException
    {
        return fileSystemDelegate.truncate(f, newLength);
    }

    @Override
    public boolean delete(Path f)
            throws IOException
    {
        return fileSystemDelegate.delete(f);
    }

    @Override
    public boolean exists(Path f)
            throws IOException
    {
        return fileSystemDelegate.exists(f);
    }

    @Override
    public boolean isDirectory(Path f)
            throws IOException
    {
        return fileSystemDelegate.isDirectory(f);
    }

    @Override
    public boolean isFile(Path f)
            throws IOException
    {
        return fileSystemDelegate.isFile(f);
    }

    @Override
    public long getLength(Path f)
            throws IOException
    {
        return fileSystemDelegate.getLength(f);
    }

    @Override
    public ContentSummary getContentSummary(Path f)
            throws IOException
    {
        return fileSystemDelegate.getContentSummary(f);
    }

    @Override
    public RemoteIterator<Path> listCorruptFileBlocks(Path path)
            throws IOException
    {
        return fileSystemDelegate.listCorruptFileBlocks(path);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
            throws FileNotFoundException, IOException
    {
        return fileSystemDelegate.listLocatedStatus(f);
    }

    @Override
    public RemoteIterator<FileStatus> listStatusIterator(Path p)
            throws FileNotFoundException, IOException
    {
        return fileSystemDelegate.listStatusIterator(p);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive)
            throws FileNotFoundException, IOException
    {
        return fileSystemDelegate.listFiles(f, recursive);
    }

    @Override
    public Path getHomeDirectory()
    {
        return fileSystemDelegate.getHomeDirectory();
    }

    @Override
    public boolean mkdirs(Path f)
            throws IOException
    {
        return fileSystemDelegate.mkdirs(f);
    }

    @Override
    public void copyFromLocalFile(Path src, Path dst)
            throws IOException
    {
        fileSystemDelegate.copyFromLocalFile(src, dst);
    }

    @Override
    public void moveFromLocalFile(Path[] srcs, Path dst)
            throws IOException
    {
        fileSystemDelegate.moveFromLocalFile(srcs, dst);
    }

    @Override
    public void moveFromLocalFile(Path src, Path dst)
            throws IOException
    {
        fileSystemDelegate.moveFromLocalFile(src, dst);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
            throws IOException
    {
        fileSystemDelegate.copyFromLocalFile(delSrc, src, dst);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst)
            throws IOException
    {
        fileSystemDelegate.copyFromLocalFile(delSrc, overwrite, srcs, dst);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst)
            throws IOException
    {
        fileSystemDelegate.copyFromLocalFile(delSrc, overwrite, src, dst);
    }

    @Override
    public void copyToLocalFile(Path src, Path dst)
            throws IOException
    {
        fileSystemDelegate.copyToLocalFile(src, dst);
    }

    @Override
    public void moveToLocalFile(Path src, Path dst)
            throws IOException
    {
        fileSystemDelegate.moveToLocalFile(src, dst);
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst)
            throws IOException
    {
        fileSystemDelegate.copyToLocalFile(delSrc, src, dst);
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem)
            throws IOException
    {
        fileSystemDelegate.copyToLocalFile(delSrc, src, dst, useRawLocalFileSystem);
    }

    @Override
    public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
            throws IOException
    {
        return fileSystemDelegate.startLocalOutput(fsOutputFile, tmpLocalFile);
    }

    @Override
    public long getBlockSize(Path f)
            throws IOException
    {
        return fileSystemDelegate.getBlockSize(f);
    }

    @Override
    public short getDefaultReplication()
    {
        return fileSystemDelegate.getDefaultReplication();
    }

    @Override
    public short getDefaultReplication(Path path)
    {
        return fileSystemDelegate.getDefaultReplication(path);
    }

    @Override
    public void access(Path path, FsAction mode)
            throws AccessControlException, FileNotFoundException, IOException
    {
        fileSystemDelegate.access(path, mode);
    }

    @Override
    public void createSymlink(Path target, Path link, boolean createParent)
            throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, IOException
    {
        fileSystemDelegate.createSymlink(target, link, createParent);
    }

    @Override
    public FileStatus getFileLinkStatus(Path f)
            throws AccessControlException, FileNotFoundException, UnsupportedFileSystemException, IOException
    {
        return fileSystemDelegate.getFileLinkStatus(f);
    }

    @Override
    public boolean supportsSymlinks()
    {
        return fileSystemDelegate.supportsSymlinks();
    }

    @Override
    public Path getLinkTarget(Path f)
            throws IOException
    {
        return fileSystemDelegate.getLinkTarget(f);
    }

    @Override
    public FileChecksum getFileChecksum(Path f)
            throws IOException
    {
        return fileSystemDelegate.getFileChecksum(f);
    }

    @Override
    public FileChecksum getFileChecksum(Path f, long length)
            throws IOException
    {
        return fileSystemDelegate.getFileChecksum(f, length);
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum)
    {
        fileSystemDelegate.setVerifyChecksum(verifyChecksum);
    }

    @Override
    public void setWriteChecksum(boolean writeChecksum)
    {
        fileSystemDelegate.setWriteChecksum(writeChecksum);
    }

    @Override
    public void setPermission(Path p, FsPermission permission)
            throws IOException
    {
        fileSystemDelegate.setPermission(p, permission);
    }

    @Override
    public void setOwner(Path p, String username, String groupname)
            throws IOException
    {
        fileSystemDelegate.setOwner(p, username, groupname);
    }

    @Override
    public void setTimes(Path p, long mtime, long atime)
            throws IOException
    {
        fileSystemDelegate.setTimes(p, mtime, atime);
    }

    @Override
    public Path createSnapshot(Path path, String snapshotName)
            throws IOException
    {
        return fileSystemDelegate.createSnapshot(path, snapshotName);
    }

    @Override
    public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName)
            throws IOException
    {
        fileSystemDelegate.renameSnapshot(path, snapshotOldName, snapshotNewName);
    }

    @Override
    public void deleteSnapshot(Path path, String snapshotName)
            throws IOException
    {
        fileSystemDelegate.deleteSnapshot(path, snapshotName);
    }

    @Override
    public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
            throws IOException
    {
        fileSystemDelegate.modifyAclEntries(path, aclSpec);
    }

    @Override
    public void removeAclEntries(Path path, List<AclEntry> aclSpec)
            throws IOException
    {
        fileSystemDelegate.removeAclEntries(path, aclSpec);
    }

    @Override
    public void removeDefaultAcl(Path path)
            throws IOException
    {
        fileSystemDelegate.removeDefaultAcl(path);
    }

    @Override
    public AclStatus getAclStatus(Path path)
            throws IOException
    {
        return fileSystemDelegate.getAclStatus(path);
    }

    @Override
    public void setStoragePolicy(Path src, String policyName)
            throws IOException
    {
        fileSystemDelegate.setStoragePolicy(src, policyName);
    }

    @Override
    public void unsetStoragePolicy(Path src)
            throws IOException
    {
        fileSystemDelegate.unsetStoragePolicy(src);
    }

    @Override
    public BlockStoragePolicySpi getStoragePolicy(Path src)
            throws IOException
    {
        return fileSystemDelegate.getStoragePolicy(src);
    }

    @Override
    public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
            throws IOException
    {
        return fileSystemDelegate.getAllStoragePolicies();
    }

    @Override
    public Path getTrashRoot(Path path)
    {
        return fileSystemDelegate.getTrashRoot(path);
    }

    @Override
    public Collection<FileStatus> getTrashRoots(boolean allUsers)
    {
        return fileSystemDelegate.getTrashRoots(allUsers);
    }

    @Override
    public StorageStatistics getStorageStatistics()
    {
        return fileSystemDelegate.getStorageStatistics();
    }

    @Override
    public FSDataOutputStreamBuilder createFile(Path path)
    {
        return fileSystemDelegate.createFile(path);
    }

    @Override
    public Configuration getConf()
    {
        return fileSystemDelegate.getConf();
    }

    @Override
    public void setConf(Configuration conf)
    {
        // fileSystemDelegate is not yet initialized when this method is called from the default constructor of FileSystem
        if (fileSystemDelegate == null) {
            super.setConf(conf);
        }
        else {
            fileSystemDelegate.setConf(conf);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        fileSystemDelegate.close();
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
