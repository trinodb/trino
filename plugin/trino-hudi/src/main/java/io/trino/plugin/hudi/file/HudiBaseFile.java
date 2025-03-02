package io.trino.plugin.hudi.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hudi.common.model.HoodieBaseFile;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public class HudiBaseFile implements HudiFile {
    private final String path;
    private final long fileSize;
    private final long modificationTime;
    private final long start;
    private final long length;

    public static HudiBaseFile of(HoodieBaseFile baseFile) {
        return of(baseFile, 0, baseFile.getFileSize());
    }

    public static HudiBaseFile of(HoodieBaseFile baseFile, long start, long length) {
        return new HudiBaseFile(baseFile, start, length);
    }

    @JsonCreator
    public HudiBaseFile(@JsonProperty("path") String path,
                        @JsonProperty("fileSize") long fileSize,
                        @JsonProperty("modificationTime") long modificationTime,
                        @JsonProperty("start") long start,
                        @JsonProperty("length") long length) {
        this.path = path;
        this.fileSize = fileSize;
        this.modificationTime = modificationTime;
        this.start = start;
        this.length = length;
    }

    private HudiBaseFile(HoodieBaseFile baseFile, long start, long length) {
        checkArgument(baseFile != null, "baseFile is null");
        checkArgument(start >= 0, "start must be positive");
        checkArgument(length >= 0, "length must be positive");
        checkArgument(start + length <= baseFile.getFileSize(), "fileSize must be at least start + length");
        this.path = baseFile.getPath();
        this.fileSize = baseFile.getFileSize();
        this.modificationTime = baseFile.getPathInfo().getModificationTime();
        this.start = start;
        this.length = length;
    }

    @JsonProperty
    public String getPath() {
        return path;
    }

    @JsonProperty
    public long getFileSize() {
        return fileSize;
    }

    @JsonProperty
    public long getModificationTime() {
        return modificationTime;
    }

    @JsonProperty
    public long getStart() {
        return start;
    }

    @JsonProperty
    public long getLength() {
        return length;
    }
}
