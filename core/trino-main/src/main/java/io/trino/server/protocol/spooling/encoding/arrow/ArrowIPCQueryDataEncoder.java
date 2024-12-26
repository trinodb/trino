package io.trino.server.protocol.spooling.encoding.arrow;

import io.trino.Session;
import io.trino.arrow.ArrowWriter;

import io.trino.client.spooling.DataAttributes;
import io.trino.server.protocol.OutputColumn;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.spi.Page;


import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class ArrowIPCQueryDataEncoder implements QueryDataEncoder {

    private final List<OutputColumn> columns;

    //todo make singleton

    public ArrowIPCQueryDataEncoder(Session session, List<OutputColumn> columns) {
        this.columns = columns;
    }

    private final String ENCODING = "arrow-ipc";
    @Override
    public DataAttributes encodeTo(OutputStream output, List<Page> pages) throws IOException {
        List<io.trino.arrow.OutputColumn> arrowColumns = columns.stream()
                .map(c -> new io.trino.arrow.OutputColumn(c.sourcePageChannel(), c.columnName(), c.type()))
                .toList();
        try(ArrowWriter arrowWriter = new ArrowWriter(arrowColumns, output)){
            for (Page page : pages) {
                arrowWriter.write(page);
            }
        }
        return  DataAttributes.builder().build();

    }

    @Override
    public String encoding() {
        return ENCODING;
    }
}
