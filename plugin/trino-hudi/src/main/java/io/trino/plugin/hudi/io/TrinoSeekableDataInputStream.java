package io.trino.plugin.hudi.io;

import io.trino.filesystem.TrinoInputStream;
import org.apache.hudi.io.SeekableDataInputStream;

import java.io.IOException;

public class TrinoSeekableDataInputStream extends SeekableDataInputStream {
    private final TrinoInputStream stream;

    public TrinoSeekableDataInputStream(TrinoInputStream stream) {
        super(stream);
        this.stream = stream;
    }

    @Override
    public long getPos() throws IOException {
        return stream.getPosition();
    }

    @Override
    public void seek(long pos) throws IOException {
        stream.seek(pos);
    }
}
