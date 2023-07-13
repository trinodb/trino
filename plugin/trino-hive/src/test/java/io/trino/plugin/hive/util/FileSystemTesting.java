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
package io.trino.plugin.hive.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static java.lang.Math.toIntExact;

// copied from org.apache.hadoop.hive.ql.io.orc.TestInputOutputFormat
@SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
public class FileSystemTesting
{
    public static class MockBlock
    {
        private int offset;
        private int length;
        private final String[] hosts;

        public MockBlock(String... hosts)
        {
            this.hosts = hosts;
        }

        public void setLength(int length)
        {
            this.length = length;
        }

        @Override
        public String toString()
        {
            StringBuilder buffer = new StringBuilder();
            buffer.append("block{offset: ");
            buffer.append(offset);
            buffer.append(", length: ");
            buffer.append(length);
            buffer.append(", hosts: [");
            for (int i = 0; i < hosts.length; i++) {
                if (i != 0) {
                    buffer.append(", ");
                }
                buffer.append(hosts[i]);
            }
            buffer.append("]}");
            return buffer.toString();
        }
    }

    public static class MockFile
    {
        private final Path path;
        private final int blockSize;
        private int length;
        private MockBlock[] blocks;
        private byte[] content;

        public MockFile(String path, int blockSize, byte[] content, MockBlock... blocks)
        {
            this.path = new Path(path);
            this.blockSize = blockSize;
            this.blocks = blocks;
            this.content = content;
            this.length = content.length;
            int offset = 0;
            for (MockBlock block : blocks) {
                block.offset = offset;
                block.length = Math.min(length - offset, blockSize);
                offset += block.length;
            }
        }

        @SuppressWarnings("NonFinalFieldReferencedInHashCode")
        @Override
        public int hashCode()
        {
            return path.hashCode() + 31 * length;
        }

        @SuppressWarnings("NonFinalFieldReferenceInEquals")
        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof MockFile)) {
                return false;
            }
            return ((MockFile) obj).path.equals(path) && ((MockFile) obj).length == length;
        }

        @Override
        public String toString()
        {
            StringBuilder buffer = new StringBuilder();
            buffer.append("mockFile{path: ");
            buffer.append(path.toString());
            buffer.append(", blkSize: ");
            buffer.append(blockSize);
            buffer.append(", len: ");
            buffer.append(length);
            buffer.append(", blocks: [");
            for (int i = 0; i < blocks.length; i++) {
                if (i != 0) {
                    buffer.append(", ");
                }
                buffer.append(blocks[i]);
            }
            buffer.append("]}");
            return buffer.toString();
        }
    }

    static class MockInputStream
            extends FSInputStream
    {
        private final MockFile file;
        private int offset;

        public MockInputStream(MockFile file)
        {
            this.file = file;
        }

        @Override
        public void seek(long offset)
        {
            this.offset = toIntExact(offset);
        }

        @Override
        public long getPos()
        {
            return offset;
        }

        @Override
        public boolean seekToNewSource(long l)
        {
            return false;
        }

        @Override
        public int read()
                throws IOException
        {
            if (offset < file.length) {
                int i = file.content[offset] & 0xff;
                offset++;
                return i;
            }
            return -1;
        }

        @Override
        public int available()
        {
            return file.length - offset;
        }
    }

    public static class MockPath
            extends Path
    {
        private final FileSystem fs;

        public MockPath(FileSystem fs, String path)
        {
            super(path);
            this.fs = fs;
        }

        @Override
        public FileSystem getFileSystem(Configuration conf)
        {
            return fs;
        }
    }

    public static class MockOutputStream
            extends FSDataOutputStream
    {
        private final MockFile file;

        public MockOutputStream(MockFile file)
        {
            super(new DataOutputBuffer(), null);
            this.file = file;
        }

        /**
         * Set the blocks and their location for the file.
         * Must be called after the stream is closed or the block length will be
         * wrong.
         *
         * @param blocks the list of blocks
         */
        public void setBlocks(MockBlock... blocks)
        {
            file.blocks = blocks;
            int offset = 0;
            int i = 0;
            while (offset < file.length && i < blocks.length) {
                blocks[i].offset = offset;
                blocks[i].length = Math.min(file.length - offset, file.blockSize);
                offset += blocks[i].length;
                i += 1;
            }
        }

        @Override
        public void close()
                throws IOException
        {
            super.close();
            DataOutputBuffer buf = (DataOutputBuffer) getWrappedStream();
            file.length = buf.getLength();
            file.content = new byte[file.length];
            MockBlock block = new MockBlock("host1");
            block.setLength(file.length);
            setBlocks(block);
            System.arraycopy(buf.getData(), 0, file.content, 0, file.length);
        }

        @Override
        public String toString()
        {
            return "Out stream to " + file.toString();
        }
    }

    public static final class MockFileSystem
            extends FileSystem
    {
        private final List<MockFile> files = new ArrayList<>();
        private final Map<MockFile, FileStatus> fileStatusMap = new HashMap<>();
        private Path workingDir = new Path("/");
        private final List<MockFile> globalFiles = new ArrayList<>();

        @Override
        public void initialize(URI uri, Configuration conf)
        {
            setConf(conf);
        }

        public MockFileSystem(Configuration conf, MockFile... files)
        {
            setConf(conf);
            this.files.addAll(Arrays.asList(files));
        }

        @Override
        public URI getUri()
        {
            try {
                return new URI("mock:///");
            }
            catch (URISyntaxException err) {
                throw new IllegalArgumentException("huh?", err);
            }
        }

        @Override
        public String getScheme()
        {
            return "mock";
        }

        @Override
        public FSDataInputStream open(Path path, int i)
                throws IOException
        {
            MockFile file = findFile(path);
            if (file != null) {
                return new FSDataInputStream(new MockInputStream(file));
            }
            throw new FileNotFoundException("File not found: " + path);
        }

        private MockFile findFile(Path path)
        {
            for (MockFile file : files) {
                if (file.path.equals(path)) {
                    return file;
                }
            }
            for (MockFile file : globalFiles) {
                if (file.path.equals(path)) {
                    return file;
                }
            }
            return null;
        }

        @Override
        public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progressable)
                throws IOException
        {
            MockFile file = findFile(path);
            if (file == null) {
                file = new MockFile(path.toString(), toIntExact(blockSize), new byte[0]);
                files.add(file);
            }
            return new MockOutputStream(file);
        }

        @Override
        public FSDataOutputStream append(Path path, int bufferSize, Progressable progressable)
                throws IOException
        {
            return create(path, FsPermission.getDefault(), true, bufferSize, (short) 3, 256 * 1024, progressable);
        }

        @Override
        public boolean rename(Path path, Path path2)
                throws IOException
        {
            return false;
        }

        @SuppressWarnings("deprecation")
        @Override
        public boolean delete(Path path)
                throws IOException
        {
            int removed = 0;
            for (int i = 0; i < files.size(); i++) {
                MockFile mf = files.get(i);
                if (path.equals(mf.path)) {
                    files.remove(i);
                    removed++;
                    break;
                }
            }
            for (int i = 0; i < globalFiles.size(); i++) {
                MockFile mf = files.get(i);
                if (path.equals(mf.path)) {
                    globalFiles.remove(i);
                    removed++;
                    break;
                }
            }
            return removed > 0;
        }

        @Override
        public boolean delete(Path path, boolean recursive)
                throws IOException
        {
            if (recursive) {
                throw new UnsupportedOperationException();
            }
            return delete(path);
        }

        @Override
        public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
                throws IOException
        {
            return new RemoteIterator<>()
            {
                private final Iterator<LocatedFileStatus> iterator = listLocatedFileStatuses(f).iterator();

                @Override
                public boolean hasNext()
                {
                    return iterator.hasNext();
                }

                @Override
                public LocatedFileStatus next()
                {
                    return iterator.next();
                }
            };
        }

        @SuppressWarnings("deprecation")
        private List<LocatedFileStatus> listLocatedFileStatuses(Path path)
        {
            path = path.makeQualified(this);
            List<LocatedFileStatus> result = new ArrayList<>();
            String pathname = path.toString();
            String pathnameAsDir = pathname + "/";
            Set<String> dirs = new TreeSet<>();
            MockFile file = findFile(path);
            if (file != null) {
                result.add(createLocatedStatus(file));
                return result;
            }
            findMatchingLocatedFiles(files, pathnameAsDir, dirs, result);
            findMatchingLocatedFiles(globalFiles, pathnameAsDir, dirs, result);
            // for each directory add it once
            for (String dir : dirs) {
                result.add(createLocatedDirectory(new MockPath(this, pathnameAsDir + dir)));
            }
            return result;
        }

        @SuppressWarnings("deprecation")
        @Override
        public FileStatus[] listStatus(Path path)
        {
            path = path.makeQualified(this);
            List<FileStatus> result = new ArrayList<>();
            String pathname = path.toString();
            String pathnameAsDir = pathname + "/";
            Set<String> dirs = new TreeSet<>();
            MockFile file = findFile(path);
            if (file != null) {
                return new FileStatus[] {createStatus(file)};
            }
            findMatchingFiles(files, pathnameAsDir, dirs, result);
            findMatchingFiles(globalFiles, pathnameAsDir, dirs, result);
            // for each directory add it once
            for (String dir : dirs) {
                result.add(createDirectory(new MockPath(this, pathnameAsDir + dir)));
            }
            return result.toArray(new FileStatus[0]);
        }

        private void findMatchingFiles(List<MockFile> files, String pathnameAsDir, Set<String> dirs, List<FileStatus> result)
        {
            for (MockFile file : files) {
                String filename = file.path.toString();
                if (filename.startsWith(pathnameAsDir)) {
                    String tail = filename.substring(pathnameAsDir.length());
                    int nextSlash = tail.indexOf('/');
                    if (nextSlash > 0) {
                        dirs.add(tail.substring(0, nextSlash));
                    }
                    else {
                        result.add(createStatus(file));
                    }
                }
            }
        }

        private void findMatchingLocatedFiles(List<MockFile> files, String pathnameAsDir, Set<String> dirs, List<LocatedFileStatus> result)
        {
            for (MockFile file : files) {
                String filename = file.path.toString();
                if (filename.startsWith(pathnameAsDir)) {
                    String tail = filename.substring(pathnameAsDir.length());
                    int nextSlash = tail.indexOf('/');
                    if (nextSlash > 0) {
                        dirs.add(tail.substring(0, nextSlash));
                    }
                    else {
                        result.add(createLocatedStatus(file));
                    }
                }
            }
        }

        @Override
        public void setWorkingDirectory(Path path)
        {
            workingDir = path;
        }

        @Override
        public Path getWorkingDirectory()
        {
            return workingDir;
        }

        @Override
        public boolean mkdirs(Path path, FsPermission fsPermission)
        {
            return false;
        }

        private FileStatus createStatus(MockFile file)
        {
            if (fileStatusMap.containsKey(file)) {
                return fileStatusMap.get(file);
            }
            FileStatus fileStatus = new FileStatus(
                    file.length,
                    false,
                    1,
                    file.blockSize,
                    0,
                    0,
                    FsPermission.createImmutable((short) 644),
                    "owen",
                    "group",
                    file.path);
            fileStatusMap.put(file, fileStatus);
            return fileStatus;
        }

        private static FileStatus createDirectory(Path dir)
        {
            return new FileStatus(
                    0,
                    true,
                    0,
                    0,
                    0,
                    0,
                    FsPermission.createImmutable((short) 755),
                    "owen",
                    "group",
                    dir);
        }

        private LocatedFileStatus createLocatedStatus(MockFile file)
        {
            FileStatus fileStatus = createStatus(file);
            return new LocatedFileStatus(fileStatus, getFileBlockLocationsImpl(fileStatus));
        }

        private LocatedFileStatus createLocatedDirectory(Path dir)
        {
            FileStatus fileStatus = createDirectory(dir);
            return new LocatedFileStatus(fileStatus, getFileBlockLocationsImpl(fileStatus));
        }

        @SuppressWarnings("deprecation")
        @Override
        public FileStatus getFileStatus(Path path)
                throws IOException
        {
            path = path.makeQualified(this);
            String pathnameAsDir = path.toString() + "/";
            MockFile file = findFile(path);
            if (file != null) {
                return createStatus(file);
            }
            for (MockFile dir : files) {
                if (dir.path.toString().startsWith(pathnameAsDir)) {
                    return createDirectory(path);
                }
            }
            for (MockFile dir : globalFiles) {
                if (dir.path.toString().startsWith(pathnameAsDir)) {
                    return createDirectory(path);
                }
            }
            throw new FileNotFoundException("File does not exist: " + path);
        }

        @Override
        public BlockLocation[] getFileBlockLocations(FileStatus stat, long start, long len)
        {
            return getFileBlockLocationsImpl(stat);
        }

        private BlockLocation[] getFileBlockLocationsImpl(FileStatus stat)
        {
            List<BlockLocation> result = new ArrayList<>();
            MockFile file = findFile(stat.getPath());
            if (file != null) {
                for (MockBlock block : file.blocks) {
                    String[] topology = new String[block.hosts.length];
                    for (int i = 0; i < topology.length; ++i) {
                        topology[i] = "/rack/ " + block.hosts[i];
                    }
                    result.add(new BlockLocation(block.hosts, block.hosts, topology, block.offset, block.length));
                }
                return result.toArray(new BlockLocation[0]);
            }
            return new BlockLocation[0];
        }

        @Override
        public String toString()
        {
            StringBuilder buffer = new StringBuilder();
            buffer.append("mockFs{files:[");
            for (int i = 0; i < files.size(); ++i) {
                if (i != 0) {
                    buffer.append(", ");
                }
                buffer.append(files.get(i));
            }
            buffer.append("]}");
            return buffer.toString();
        }
    }
}
