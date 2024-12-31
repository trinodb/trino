package io.trino.adbc;

import io.trino.client.QueryData;
import io.trino.client.QueryStats;
import io.trino.client.StatementClient;
import io.trino.client.spooling.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorUnloader;

import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public class SpooledResultReader extends ArrowReader {
    private final StatementClient statementClient;
    private final SegmentLoader segmentLoader;
    private Iterator<Segment> segments;
    private VectorSchemaRoot currentRoot;
    private Schema schema;


    public SpooledResultReader(BufferAllocator bufferAllocator, StatementClient statementClient, SegmentLoader segmentLoader, Consumer<QueryStats> progressCallback) {
        super(bufferAllocator);
        this.statementClient = statementClient;
        this.segmentLoader = segmentLoader;
        Iterator<Segment> segments = new AsyncSegmentIterator(statementClient, progressCallback, Optional.empty());
    }

    public VectorSchemaRoot loadSegment(Segment segment) throws IOException {
        if (segment instanceof SpooledSegment) {
            SpooledSegment spooledSegment = (SpooledSegment) segment;
            InputStream segmentStream = segmentLoader.load(spooledSegment);
            ArrowStreamReader arrowStreamReader = new ArrowStreamReader(segmentStream, allocator);
            return arrowStreamReader.getVectorSchemaRoot();
        }else if (segment instanceof InlineSegment) {
            InlineSegment inlineSegment = (InlineSegment) segment;
            InputStream segmentStream = new ByteArrayInputStream(inlineSegment.getData());
            ArrowStreamReader arrowStreamReader = new ArrowStreamReader(segmentStream, allocator);
            return arrowStreamReader.getVectorSchemaRoot();
        }else{
            throw new IllegalArgumentException("Not a valid segment");
        }
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        if (!segments.hasNext()) {
            return false;
        }
        VectorSchemaRoot vsr = loadSegment(segments.next());
        schema = vsr.getSchema();
        ensureInitialized();
        VectorUnloader unloader = new VectorUnloader(vsr);
        loadRecordBatch(unloader.getRecordBatch());
        return true;

    }

    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {
        statementClient.close();

    }

    @Override
    protected Schema readSchema() throws IOException {
        return schema;
    }
}
