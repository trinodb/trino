package tech.ydb.trino;

import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestYdbWriteMetadata {
    @Test
    public void testHiddenKeyWithPrimaryKeyNamedColumn() {
        TestingYdbJdbcClient client = new TestingYdbJdbcClient(new BaseJdbcConfig(),
                _ -> {
                    throw new SQLException("This test must not open a connection");
                },
                new YdbQueryBuilder(RemoteQueryModifier.NONE), new DefaultIdentifierMapping(), RemoteQueryModifier.NONE);
        ConnectorTableMetadata metadata = new ConnectorTableMetadata(
                new SchemaTableName("default", "fixture"), List.of(new ColumnMetadata("primary key", BIGINT)));
        assertThat(client.createTableSqls(
                new RemoteTableName(Optional.empty(), Optional.empty(), "fixture"),
                List.of("`primary key` Int64"), metadata))
                .containsExactly("CREATE TABLE `fixture` (`primary key` Int64, `_ydb_trino_test_pk` Serial, PRIMARY KEY (`_ydb_trino_test_pk`))");
    }

    @Test
    public void testStagingUsesDriverTableIdentity() throws Exception {
        Map<String, Object> row = Map.of(
                "TABLE_NAME", "source",
                "COLUMN_NAME", "payload",
                "DATA_TYPE", Types.BIGINT,
                "TYPE_NAME", "Int64",
                "COLUMN_SIZE", 19,
                "DECIMAL_DIGITS", 0,
                "NULLABLE", DatabaseMetaData.columnNullable);
        AtomicBoolean firstRow = new AtomicBoolean(true);
        ResultSet result = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
                (_, method, args) -> switch (method.getName()) {
                    case "next" -> firstRow.getAndSet(false);
                    case "getString", "getInt" -> row.get(args[0]);
                    case "wasNull" -> false;
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        DatabaseMetaData metadata = (DatabaseMetaData) Proxy.newProxyInstance(DatabaseMetaData.class.getClassLoader(), new Class<?>[]{DatabaseMetaData.class},
                (_, method, _) -> switch (method.getName()) {
                    case "getColumns" -> result;
                    case "getSearchStringEscape" -> "\\";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        List<String> commands = new ArrayList<>();
        Statement statement = (Statement) Proxy.newProxyInstance(Statement.class.getClassLoader(), new Class<?>[]{Statement.class},
                (_, method, args) -> {
                    if (method.getName().equals("execute")) {
                        commands.add((String) args[0]);
                        return false;
                    }
                    assertThat(method.getName()).isEqualTo("close");
                    return null;
                });
        Supplier<Connection> connection = () -> (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                (_, method, _) -> switch (method.getName()) {
                    case "getMetaData" -> metadata;
                    case "createStatement" -> statement;
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        YdbClient client = new YdbClient(new BaseJdbcConfig(), _ -> connection.get(),
                new YdbQueryBuilder(RemoteQueryModifier.NONE), new DefaultIdentifierMapping(), RemoteQueryModifier.NONE);
        try (Connection target = connection.get()) {
            client.copyTableSchema(SESSION, target, null, "default", "source", "staging", List.of("payload"));
        }
        assertThat(commands).containsExactly(
                "CREATE TABLE `staging` (`payload` Int64, `_trino_ydb_staging_key` Serial, PRIMARY KEY (`_trino_ydb_staging_key`))");
    }
}
