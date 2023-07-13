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
package io.trino.hdfs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemCache;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.FileSystem.getFileSystemClass;
import static org.apache.hadoop.security.UserGroupInformationShim.getSubject;

public class TrinoFileSystemCache
        implements FileSystemCache
{
    private static final Logger log = Logger.get(TrinoFileSystemCache.class);

    public static final String CACHE_KEY = "fs.cache.credentials";

    public static final TrinoFileSystemCache INSTANCE = new TrinoFileSystemCache();

    private final AtomicLong unique = new AtomicLong();

    private final TrinoFileSystemCacheStats stats;

    private final Map<FileSystemKey, FileSystemHolder> cache = new ConcurrentHashMap<>();
    /*
     * ConcurrentHashMap has a lock per partitioned key-space bucket, and hence there is no consistent
     * or 'serialized' view of current number of entries in the map from a thread that would like to
     * add/delete/update an entry. As we have to limit the max size of the cache to 'fs.cache.max-size',
     * an auxiliary variable `cacheSize` is used to track the 'serialized' view of entry count in the cache.
     * cacheSize should only be updated after acquiring cache's partition lock (eg: from inside cache.compute())
     */
    private final AtomicLong cacheSize = new AtomicLong();

    @VisibleForTesting
    TrinoFileSystemCache()
    {
        this.stats = new TrinoFileSystemCacheStats(cache::size);
    }

    @Override
    public FileSystem get(URI uri, Configuration conf)
            throws IOException
    {
        stats.newGetCall();
        return getInternal(uri, conf, 0);
    }

    @Override
    public FileSystem getUnique(URI uri, Configuration conf)
            throws IOException
    {
        stats.newGetUniqueCall();
        return getInternal(uri, conf, unique.incrementAndGet());
    }

    @VisibleForTesting
    int getCacheSize()
    {
        return cache.size();
    }

    private FileSystem getInternal(URI uri, Configuration conf, long unique)
            throws IOException
    {
        UserGroupInformation userGroupInformation = UserGroupInformation.getCurrentUser();
        FileSystemKey key = createFileSystemKey(uri, userGroupInformation, unique);
        Set<?> privateCredentials = getPrivateCredentials(userGroupInformation);

        int maxSize = conf.getInt("fs.cache.max-size", 1000);
        FileSystemHolder fileSystemHolder;
        try {
            fileSystemHolder = cache.compute(key, (k, currentFileSystemHolder) -> {
                if (currentFileSystemHolder == null) {
                    // ConcurrentHashMap.compute guarantees that remapping function is invoked at most once, so cacheSize remains eventually consistent with cache.size()
                    if (cacheSize.getAndUpdate(currentSize -> Math.min(currentSize + 1, maxSize)) >= maxSize) {
                        throw new RuntimeException(
                                new IOException(format("FileSystem max cache size has been reached: %s", maxSize)));
                    }
                    return new FileSystemHolder(conf, privateCredentials);
                }
                // Update file system instance when credentials change.
                if (currentFileSystemHolder.credentialsChanged(uri, conf, privateCredentials)) {
                    return new FileSystemHolder(conf, privateCredentials);
                }
                return currentFileSystemHolder;
            });

            // Now create the filesystem object outside of cache's lock
            fileSystemHolder.createFileSystemOnce(uri, conf);
        }
        catch (RuntimeException | IOException e) {
            stats.newGetCallFailed();
            throwIfInstanceOf(e, IOException.class);
            throwIfInstanceOf(e.getCause(), IOException.class);
            throw e;
        }

        return fileSystemHolder.getFileSystem();
    }

    private static FileSystem createFileSystem(URI uri, Configuration conf)
            throws IOException
    {
        Class<?> clazz = getFileSystemClass(uri.getScheme(), conf);
        if (clazz == null) {
            throw new IOException("No FileSystem for scheme: " + uri.getScheme());
        }
        FileSystem original = (FileSystem) ReflectionUtils.newInstance(clazz, conf);
        original.initialize(uri, conf);
        FilterFileSystem wrapper = new FileSystemWrapper(original);
        FileSystemFinalizerService.getInstance().addFinalizer(wrapper, () -> {
            try {
                closeFileSystem(original);
            }
            catch (IOException e) {
                log.error(e, "Error occurred when finalizing file system");
            }
        });
        return wrapper;
    }

    @Override
    public void remove(FileSystem fileSystem)
    {
        stats.newRemoveCall();
        cache.forEach((key, fileSystemHolder) -> {
            if (fileSystem.equals(fileSystemHolder.getFileSystem())) {
                // After acquiring the lock, decrement cacheSize only if
                // (1) the key is still mapped to a FileSystemHolder
                // (2) the filesystem object inside FileSystemHolder is the same
                cache.compute(key, (k, currentFileSystemHolder) -> {
                    if (currentFileSystemHolder != null
                            && fileSystem.equals(currentFileSystemHolder.getFileSystem())) {
                        cacheSize.decrementAndGet();
                        return null;
                    }
                    return currentFileSystemHolder;
                });
            }
        });
    }

    @Override
    public void closeAll()
            throws IOException
    {
        try {
            cache.forEach((key, fileSystemHolder) -> {
                try {
                    cache.compute(key, (k, currentFileSystemHolder) -> {
                        // decrement cacheSize only if the key is still mapped
                        if (currentFileSystemHolder != null) {
                            cacheSize.decrementAndGet();
                        }
                        return null;
                    });
                    FileSystem fs = fileSystemHolder.getFileSystem();
                    if (fs != null) {
                        closeFileSystem(fs);
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        catch (RuntimeException e) {
            throwIfInstanceOf(e.getCause(), IOException.class);
            throw e;
        }
    }

    @SuppressModernizer
    private static void closeFileSystem(FileSystem fileSystem)
            throws IOException
    {
        fileSystem.close();
    }

    private static FileSystemKey createFileSystemKey(URI uri, UserGroupInformation userGroupInformation, long unique)
    {
        String scheme = nullToEmpty(uri.getScheme()).toLowerCase(ENGLISH);
        String authority = nullToEmpty(uri.getAuthority()).toLowerCase(ENGLISH);
        String realUser;
        String proxyUser;
        AuthenticationMethod authenticationMethod = userGroupInformation.getAuthenticationMethod();
        switch (authenticationMethod) {
            case SIMPLE, KERBEROS -> {
                realUser = userGroupInformation.getUserName();
                proxyUser = null;
            }
            case PROXY -> {
                realUser = userGroupInformation.getRealUser().getUserName();
                proxyUser = userGroupInformation.getUserName();
            }
            default -> throw new IllegalArgumentException("Unsupported authentication method: " + authenticationMethod);
        }
        return new FileSystemKey(scheme, authority, unique, realUser, proxyUser);
    }

    private static Set<?> getPrivateCredentials(UserGroupInformation userGroupInformation)
    {
        AuthenticationMethod authenticationMethod = userGroupInformation.getAuthenticationMethod();
        return switch (authenticationMethod) {
            case SIMPLE -> ImmutableSet.of();
            case KERBEROS -> ImmutableSet.copyOf(getSubject(userGroupInformation).getPrivateCredentials());
            case PROXY -> getPrivateCredentials(userGroupInformation.getRealUser());
            default -> throw new IllegalArgumentException("Unsupported authentication method: " + authenticationMethod);
        };
    }

    private static boolean isHdfs(URI uri)
    {
        String scheme = uri.getScheme();
        return "hdfs".equals(scheme) || "viewfs".equals(scheme);
    }

    @SuppressWarnings("unused")
    private record FileSystemKey(String scheme, String authority, long unique, String realUser, String proxyUser)
    {
        private FileSystemKey
        {
            requireNonNull(scheme, "scheme is null");
            requireNonNull(authority, "authority is null");
            requireNonNull(realUser, "realUser");
        }
    }

    private static class FileSystemHolder
    {
        private final Set<?> privateCredentials;
        private final String cacheCredentials;
        private volatile FileSystem fileSystem;

        public FileSystemHolder(Configuration conf, Set<?> privateCredentials)
        {
            this.privateCredentials = ImmutableSet.copyOf(requireNonNull(privateCredentials, "privateCredentials is null"));
            this.cacheCredentials = conf.get(CACHE_KEY, "");
        }

        public void createFileSystemOnce(URI uri, Configuration conf)
                throws IOException
        {
            if (fileSystem == null) {
                synchronized (this) {
                    if (fileSystem == null) {
                        fileSystem = createFileSystem(uri, conf);
                    }
                }
            }
        }

        public boolean credentialsChanged(URI newUri, Configuration newConf, Set<?> newPrivateCredentials)
        {
            // - Private credentials are only set when using Kerberos authentication.
            // When the user is the same, but the private credentials are different,
            // that means that Kerberos ticket has expired and re-login happened.
            // To prevent cache leak in such situation, the privateCredentials are not
            // a part of the FileSystemKey, but part of the FileSystemHolder. When a
            // Kerberos re-login occurs, re-create the file system and cache it using
            // the same key.
            // - Extra credentials are used to authenticate with certain file systems.
            return (isHdfs(newUri) && !privateCredentials.equals(newPrivateCredentials))
                    || !cacheCredentials.equals(newConf.get(CACHE_KEY, ""));
        }

        public FileSystem getFileSystem()
        {
            return fileSystem;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("fileSystem", fileSystem)
                    .add("privateCredentials", privateCredentials)
                    .add("cacheCredentials", cacheCredentials)
                    .toString();
        }
    }

    private static class FileSystemWrapper
            extends FilterFileSystem
    {
        public FileSystemWrapper(FileSystem fs)
        {
            super(fs);
        }

        @Override
        public FSDataInputStream open(Path f, int bufferSize)
                throws IOException
        {
            return new InputStreamWrapper(getRawFileSystem().open(f, bufferSize), this);
        }

        @Override
        public String getScheme()
        {
            return getRawFileSystem().getScheme();
        }

        @Override
        public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
                throws IOException
        {
            return new OutputStreamWrapper(getRawFileSystem().append(f, bufferSize, progress), this);
        }

        @Override
        public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
                throws IOException
        {
            return new OutputStreamWrapper(getRawFileSystem().create(f, permission, overwrite, bufferSize, replication, blockSize, progress), this);
        }

        @Override
        public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt)
                throws IOException
        {
            return new OutputStreamWrapper(getRawFileSystem().create(f, permission, flags, bufferSize, replication, blockSize, progress, checksumOpt), this);
        }

        @Override
        public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress)
                throws IOException
        {
            return new OutputStreamWrapper(getRawFileSystem().createNonRecursive(f, permission, flags, bufferSize, replication, blockSize, progress), this);
        }

        // missing in FilterFileSystem (HADOOP-16399)
        @Override
        public BlockLocation[] getFileBlockLocations(Path p, long start, long len)
                throws IOException
        {
            return fs.getFileBlockLocations(p, start, len);
        }

        // missing in FilterFileSystem
        @Override
        public RemoteIterator<LocatedFileStatus> listFiles(Path path, boolean recursive)
                throws IOException
        {
            return new RemoteIteratorWrapper(fs.listFiles(path, recursive), this);
        }
    }

    private static class OutputStreamWrapper
            extends FSDataOutputStream
    {
        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        // Keep reference to FileSystemWrapper which owns the FSDataOutputStream.
        // Otherwise, GC on FileSystemWrapper could trigger finalizer that closes wrapped FileSystem object and that would break
        // FSDataOutputStream delegate.
        private final FileSystemWrapper owningFileSystemWrapper;

        public OutputStreamWrapper(FSDataOutputStream delegate, FileSystemWrapper owningFileSystemWrapper)
        {
            super(delegate, null, delegate.getPos());
            this.owningFileSystemWrapper = requireNonNull(owningFileSystemWrapper, "owningFileSystemWrapper is null");
        }

        @Override
        public OutputStream getWrappedStream()
        {
            return ((FSDataOutputStream) super.getWrappedStream()).getWrappedStream();
        }
    }

    private static class InputStreamWrapper
            extends FSDataInputStream
    {
        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        // Keep reference to FileSystemWrapper which owns the FSDataInputStream.
        // Otherwise, GC on FileSystemWrapper could trigger finalizer that closes wrapped FileSystem object and that would break
        // FSDataInputStream delegate.
        private final FileSystemWrapper owningFileSystemWrapper;

        public InputStreamWrapper(FSDataInputStream inputStream, FileSystemWrapper owningFileSystemWrapper)
        {
            super(inputStream);
            this.owningFileSystemWrapper = requireNonNull(owningFileSystemWrapper, "owningFileSystemWrapper is null");
        }

        @Override
        public InputStream getWrappedStream()
        {
            return ((FSDataInputStream) super.getWrappedStream()).getWrappedStream();
        }
    }

    private static class RemoteIteratorWrapper
            implements RemoteIterator<LocatedFileStatus>
    {
        private final RemoteIterator<LocatedFileStatus> delegate;
        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        // Keep reference to FileSystemWrapper which owns the RemoteIterator.
        // Otherwise, GC on FileSystemWrapper could trigger finalizer that closes wrapped FileSystem object and that would break
        // RemoteIterator delegate.
        private final FileSystemWrapper owningFileSystemWrapper;

        public RemoteIteratorWrapper(RemoteIterator<LocatedFileStatus> delegate, FileSystemWrapper owningFileSystemWrapper)
        {
            this.delegate = delegate;
            this.owningFileSystemWrapper = requireNonNull(owningFileSystemWrapper, "owningFileSystemWrapper is null");
        }

        @Override
        public boolean hasNext()
                throws IOException
        {
            return delegate.hasNext();
        }

        @Override
        public LocatedFileStatus next()
                throws IOException
        {
            return delegate.next();
        }
    }

    public TrinoFileSystemCacheStats getFileSystemCacheStats()
    {
        return stats;
    }
}
