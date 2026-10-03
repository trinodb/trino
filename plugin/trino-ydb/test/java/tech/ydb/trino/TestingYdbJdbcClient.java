package tech.ydb.trino;

import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class TestingYdbJdbcClient extends YdbClient {
    static final String YDB_HIDDEN_PK_COLUMN = "_ydb_trino_test_pk";

    public TestingYdbJdbcClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier) {
        super(config, connectionFactory, queryBuilder, identifierMapping, remoteQueryModifier);
    }

    @Override
    public List<JdbcColumnHandle> getColumns(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            RemoteTableName remoteTableName) {
        return super.getColumns(session, schemaTableName, remoteTableName)
                .stream()
                .filter(column -> !column.getColumnName().equals(YDB_HIDDEN_PK_COLUMN))
                .toList();
    }

    @Override
    protected List<JdbcColumnHandle> getColumnsForPrimaryKeyLookup(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            RemoteTableName remoteTableName) {
        return super.getColumns(session, schemaTableName, remoteTableName);
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle) {
        Map<String, Object> properties = super.getTableProperties(session, tableHandle);
        if (YdbTableProperties.getPrimaryKey(properties).equals(List.of(YDB_HIDDEN_PK_COLUMN))) {
            return Map.of();
        }
        return properties;
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata) {
        if (tableMetadata.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with table comment");
        }
        String tableName = quoted(remoteTableName);
        String columnsDeclaration = String.join(", ", columns);

        boolean hasHiddenPkColumn = columns.stream()
                .anyMatch(col -> col.startsWith(quoted(YDB_HIDDEN_PK_COLUMN)) || col.startsWith(YDB_HIDDEN_PK_COLUMN));

        String sql;
        if (hasHiddenPkColumn) {
            sql = String.format("CREATE TABLE %s (%s, PRIMARY KEY (%s))", tableName, columnsDeclaration, quoted(YDB_HIDDEN_PK_COLUMN));
        } else {
            String hiddenPkColumn = quoted(YDB_HIDDEN_PK_COLUMN) + " Serial";
            sql = String.format("CREATE TABLE %s (%s, %s, PRIMARY KEY (%s))", tableName, columnsDeclaration, hiddenPkColumn, quoted(YDB_HIDDEN_PK_COLUMN));
        }
        return List.of(sql);
    }
}
