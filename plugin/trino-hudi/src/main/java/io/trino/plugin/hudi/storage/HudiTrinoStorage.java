package io.trino.plugin.hudi.storage;

import io.airlift.units.DataSize;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hudi.io.TrinoSeekableDataInputStream;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class HudiTrinoStorage extends HoodieStorage
{
    private static final int DEFAULT_BLOCK_SIZE = (int) DataSize.of(32, MEGABYTE).toBytes();
    private static final int DEFAULT_BUFFER_SIZE = 4096;
    private static final int DEFAULT_REPLICATION = 1;

    private final TrinoFileSystem fileSystem;
    private final TrinoStorageConfiguration storageConf;

    public HudiTrinoStorage(TrinoFileSystem fileSystem, TrinoStorageConfiguration storageConf) {
        super(storageConf);
        this.fileSystem = fileSystem;
        this.storageConf = storageConf;
    }

    public static Location convertToLocation(StoragePath path) {
        return Location.of(path.toString());
    }

    public static StoragePath convertToPath(Location location) {
        return new StoragePath(location.toString());
    }

    public static StoragePathInfo convertToPathInfo(FileEntry fileEntry) {
        return new StoragePathInfo(
                convertToPath(fileEntry.location()),
                fileEntry.length(),
                false,
                (short) 0,
                0,
                fileEntry.lastModified().toEpochMilli());
    }

    @Override
    public HoodieStorage newInstance(StoragePath path, StorageConfiguration<?> storageConf) {
        return this;
    }

    @Override
    public String getScheme() {
        // TODO(yihua): not used in read path
        return "file";
    }

    @Override
    public int getDefaultBlockSize(StoragePath path) {
        return DEFAULT_BLOCK_SIZE;
    }

    @Override
    public int getDefaultBufferSize() {
        return DEFAULT_BUFFER_SIZE;
    }

    @Override
    public short getDefaultReplication(StoragePath path) {
        return DEFAULT_REPLICATION;
    }

    @Override
    public URI getUri() {
        return URI.create("");
    }

    @Override
    public OutputStream create(StoragePath path, boolean overwrite) throws IOException {
        return fileSystem.newOutputFile(convertToLocation(path)).create();
    }

    @Override
    public OutputStream create(StoragePath path, boolean overwrite, Integer bufferSize, Short replication, Long sizeThreshold) throws IOException {
        return create(path, overwrite);
    }

    @Override
    public InputStream open(StoragePath path) throws IOException {
        return fileSystem.newInputFile(convertToLocation(path)).newStream();
    }

    @Override
    public SeekableDataInputStream openSeekable(StoragePath path, int bufferSize, boolean wrapStream) throws IOException {
        return new TrinoSeekableDataInputStream(
                fileSystem.newInputFile(convertToLocation(path)).newStream());
    }

    @Override
    public OutputStream append(StoragePath path) throws IOException {
        throw new UnsupportedOperationException("HudiTrinoStorage does not support append operation.");
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        return fileSystem.newInputFile(convertToLocation(path)).exists();
    }

    @Override
    public StoragePathInfo getPathInfo(StoragePath path) throws IOException {
        Location location = convertToLocation(path);
        Optional<Boolean> result = fileSystem.directoryExists(location);
        if (result.isPresent() && result.get()) {
            return new StoragePathInfo(path, 0, true, (short) 0, 0, 0);
        }
        TrinoInputFile inputFile = fileSystem.newInputFile(location);
        if (!inputFile.exists()) {
            throw new FileNotFoundException("Path " + path + " does not exist");
        }
        return new StoragePathInfo(path, inputFile.length(), false, (short) 0, 0, inputFile.lastModified().toEpochMilli());
    }

    @Override
    public boolean createDirectory(StoragePath path) throws IOException {
        fileSystem.createDirectory(convertToLocation(path));
        return true;
    }

    @Override
    public List<StoragePathInfo> listDirectEntries(StoragePath path) throws IOException {
        FileIterator fileIterator = fileSystem.listFiles(convertToLocation(path));
        List<StoragePathInfo> fileList = new ArrayList<>();
        while (fileIterator.hasNext()) {
            fileList.add(convertToPathInfo(fileIterator.next()));
        }
        return fileList;
    }

    @Override
    public List<StoragePathInfo> listFiles(StoragePath path) throws IOException {
        FileIterator fileIterator = fileSystem.listFiles(convertToLocation(path));
        List<StoragePathInfo> fileList = new ArrayList<>();
        while (fileIterator.hasNext()) {
            fileList.add(convertToPathInfo(fileIterator.next()));
        }
        return fileList;
    }

    @Override
    public List<StoragePathInfo> listDirectEntries(StoragePath path, StoragePathFilter filter) throws IOException {
        FileIterator fileIterator = fileSystem.listFiles(convertToLocation(path));
        List<StoragePathInfo> fileList = new ArrayList<>();
        while (fileIterator.hasNext()) {
            FileEntry entry = fileIterator.next();
            if (filter.accept(new StoragePath(entry.location().toString()))) {
                fileList.add(convertToPathInfo(entry));
            }
        }
        return fileList;
    }

    @Override
    public List<StoragePathInfo> globEntries(StoragePath pathPattern, StoragePathFilter filter) throws IOException {
        throw new UnsupportedOperationException("HudiTrinoStorage does not support globEntries operation.");
    }

    @Override
    public boolean rename(StoragePath oldPath, StoragePath newPath) throws IOException {
        fileSystem.renameFile(convertToLocation(oldPath), convertToLocation(newPath));
        return true;
    }

    @Override
    public boolean deleteDirectory(StoragePath path) throws IOException {
        fileSystem.deleteDirectory(convertToLocation(path));
        return true;
    }

    @Override
    public boolean deleteFile(StoragePath path) throws IOException {
        fileSystem.deleteFile(convertToLocation(path));
        return true;
    }

    @Override
    public Object getFileSystem() {
        return fileSystem;
    }

    @Override
    public HoodieStorage getRawStorage() {
        return this;
    }

    @Override
    public void close() throws IOException {
    }
}
