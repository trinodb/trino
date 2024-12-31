package io.trino.server.protocol.spooling.encoding.arrow;

import com.fasterxml.jackson.core.JsonFactory;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.arrow.ArrowWriter;

import io.trino.client.spooling.DataAttributes;
import io.trino.server.protocol.OutputColumn;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.encoding.JsonQueryDataEncoder;
import io.trino.spi.Page;


import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.plugin.base.util.JsonUtils.jsonFactory;
import static java.lang.Math.toIntExact;

public class ArrowIPCQueryDataEncoder implements QueryDataEncoder {

    private final List<OutputColumn> columns;

    //todo make singleton

    public ArrowIPCQueryDataEncoder(Session session, List<OutputColumn> columns) {
        this.columns = columns;
    }

    private static final String ENCODING = "arrow-ipc";
    @Override
    public DataAttributes encodeTo(OutputStream output, List<Page> pages) throws IOException {
        List<io.trino.arrow.OutputColumn> arrowColumns = columns.stream()
                .map(c -> new io.trino.arrow.OutputColumn(c.sourcePageChannel(), c.columnName(), c.type()))
                .toList();
        try(CountingOutputStream wrapper = new CountingOutputStream(output); ArrowWriter arrowWriter = new ArrowWriter(arrowColumns, wrapper)){
            for (Page page : pages) {
                arrowWriter.write(page);
            }
            return DataAttributes.builder()
                    .set(SEGMENT_SIZE, toIntExact(wrapper.getCount()))
                    .build();
        }


    }

    @Override
    public String encoding() {
        return ENCODING;
    }

    public static class Factory
            implements QueryDataEncoder.Factory
    {

        @Inject
        public Factory()
        {

        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new ArrowIPCQueryDataEncoder(session, columns);
        }

        @Override
        public String encoding()
        {
            return ENCODING;
        }
    }
}
