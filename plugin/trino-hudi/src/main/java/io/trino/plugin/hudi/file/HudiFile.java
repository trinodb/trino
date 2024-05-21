package io.trino.plugin.hudi.file;

public interface HudiFile {
    String getPath();

    long getFileSize();

    long getModificationTime();

    long getStart();

    long getLength();
}
