package com.aliyun.odps.cupid.trino;

import com.aliyun.odps.cupid.table.v1.writer.TableWriteSession;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorInsertTableHandle;

public class OdpsInsertTableHandle
        extends OdpsTableHandle
        implements ConnectorInsertTableHandle {

    private final String writeSessionInfo;
    private final String tableApiProvider;
    private transient TableWriteSession tableWriteSession;

    @JsonCreator
    public OdpsInsertTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("odpsTable") OdpsTable odpsTable,
            @JsonProperty("writeSessionInfo") String writeSessionInfo,
            @JsonProperty("tableApiProvider") String tableApiProvider) {
        this(schemaName, tableName, odpsTable, writeSessionInfo, tableApiProvider, null);
    }

    public OdpsInsertTableHandle(
            String schemaName,
            String tableName,
            OdpsTable odpsTable,
            String writeSessionInfo,
            String tableApiProvider,
            TableWriteSession tableWriteSession) {
        super(schemaName, tableName, odpsTable, ImmutableList.of());
        this.writeSessionInfo = writeSessionInfo;
        this.tableApiProvider = tableApiProvider;
        this.tableWriteSession = tableWriteSession;
    }

    @JsonProperty
    public String getWriteSessionInfo() {
        return writeSessionInfo;
    }

    @JsonProperty
    public String getTableApiProvider() {
        return tableApiProvider;
    }

    public TableWriteSession getTableWriteSession() {
        return tableWriteSession;
    }

}
