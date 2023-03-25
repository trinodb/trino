package io.trino.plugin.spanner;

import com.google.common.base.Preconditions;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.StandardColumnMappings;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.lang.String.join;

public class SpannerClient
        extends BaseJdbcClient
{
    private final String DEFAULT_SCHEMA = "public";
    private final SpannerConfig config;

    public SpannerClient(BaseJdbcConfig config, SpannerConfig spannerConfig, JdbcStatisticsConfig statisticsConfig, ConnectionFactory connectionFactory, QueryBuilder queryBuilder, TypeManager typeManager, IdentifierMapping identifierMapping, RemoteQueryModifier queryModifier)
    {
        super("`", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);
        this.config = spannerConfig;
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        return Optional.of(StandardColumnMappings.varcharColumnMapping(VarcharType.VARCHAR, true));
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT) {
            return WriteMapping.longMapping("INT64", bigintWriteFunction());
        }
        return WriteMapping.sliceMapping("STRING(MAX)", varcharWriteFunction());
    }

    @Override
    protected Optional<List<String>> getTableTypes()
    {
        return Optional.of(Arrays.asList("BASE TABLE", "VIEW"));
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        Set<String> schemas = new HashSet<>(Collections.singleton(DEFAULT_SCHEMA));

        try {
            ResultSet resultSet = connection.getMetaData().getSchemas(null, null);
            while (resultSet.next()) {
                schemas.add(resultSet.getString(1));
            }
            return schemas;
        }
        catch (SQLException e) {
            return schemas;
        }
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schema)
    {
        System.out.println("Called schemaExists " + schema);
        if (schema.equalsIgnoreCase(DEFAULT_SCHEMA)) {
            return true;
        }
        else {
            try {
                Connection connection = connectionFactory.openConnection(session);
                ResultSet schemas = connection.getMetaData().getSchemas(null, null);
                boolean found = false;
                while (schemas.next()) {
                    if (schemas.getString(1).equalsIgnoreCase(schema)) {
                        found = true;
                        break;
                    }
                }
                return found;
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(SpannerErrorCode.SPANNER_ERROR_CODE, "Spanner connector does not support creating schemas");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(SpannerErrorCode.SPANNER_ERROR_CODE, "Spanner connector does not support dropping schemas");
    }

    @Override
    public List<SchemaTableName> getTableNames(ConnectorSession session, Optional<String> schema)
    {
        System.out.println("Called get table names " + schema);
        List<SchemaTableName> tables = new ArrayList<>();
        try {
            Connection connection = connectionFactory.openConnection(session);
            ResultSet resultSet = getTablesFromSpanner(connection);
            while (resultSet.next()) {
                tables.add(new SchemaTableName(DEFAULT_SCHEMA, resultSet.getString(1)));
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return tables;
    }

    private ResultSet getTablesFromSpanner(Connection connection)
            throws SQLException
    {
        return connection.createStatement().executeQuery("SELECT\n" +
                "  TABLE_NAME\n" +
                "FROM\n" +
                "  INFORMATION_SCHEMA.TABLES\n" +
                "WHERE\n" +
                "  TABLE_CATALOG = '' and TABLE_SCHEMA = ''");
    }

    @Override
    protected String createTableSql(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        System.out.println("Called create table sql");
        Map<String, Object> properties = tableMetadata.getProperties();
        String primaryKey = SpannerTableProperties.getPrimaryKey(properties);
        Preconditions.checkArgument(primaryKey != null, "Primary key is required to create a table in spanner");
        String interleaveTable = SpannerTableProperties.getInterleaveInParent(properties);
        boolean onDeleteCascade = SpannerTableProperties.getOnDeleteCascade(properties);
        String interleaveClause = "";
        String onDeleteClause = "";
        if (interleaveTable != null) {
            interleaveClause = String.format(", INTERLEAVE IN PARENT %s", quoted(interleaveTable));
            onDeleteClause = onDeleteCascade ? " ON DELETE CASCADE" : " ON DELETE NO ACTION";
        }
        System.out.println("primary key " + primaryKey);
        String format = format("CREATE TABLE %s (%s) PRIMARY KEY (%s) %s %s",
                quoted(remoteTableName.getTableName()), join(", ", columns), quoted(primaryKey),
                interleaveClause, onDeleteClause);
        System.out.println(format);
        return format;
    }

    public boolean checkTableExists(ConnectorSession session, String tableName)
            throws SQLException
    {
        return checkTableExists(connectionFactory.openConnection(session), tableName);
    }

    public boolean checkTableExists(Connection connection, String tableName)
            throws SQLException
    {
        ResultSet tablesFromSpanner = getTablesFromSpanner(connection);
        boolean exists = false;
        while (tablesFromSpanner.next()) {
            String table = tablesFromSpanner.getString(1);
            if (table.equalsIgnoreCase(tableName)) {
                exists = true;
                break;
            }
        }
        return exists;
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        boolean tableExists = false;
        try {
            tableExists = checkTableExists(session, schemaTableName.getTableName());
            if (tableExists) {
                return Optional.of(new JdbcTableHandle(new SchemaTableName(DEFAULT_SCHEMA, schemaTableName.getTableName()),
                        new RemoteTableName(Optional.empty(),
                                Optional.empty(), schemaTableName.getTableName()),
                        Optional.empty()));
            }
            else {
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new TrinoException(SpannerErrorCode.SPANNER_ERROR_CODE, e);
        }
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle handle)
    {
        System.out.println("Drop table ");
        SchemaTableName schemaTableName = handle.getRequiredNamedRelation().getSchemaTableName();
        try (Connection connection = connectionFactory.openConnection(session)) {
            String format = format("DROP TABLE %s", schemaTableName.getTableName());
            System.out.println("EEX " + format);
            connection.createStatement().executeUpdate(format);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        System.out.println("PROPS WAS CALLED ");
        return new HashMap<>();
    }

    public enum SpannerErrorCode
            implements ErrorCodeSupplier
    {
        SPANNER_ERROR_CODE(1, INTERNAL_ERROR);

        private final ErrorCode errorCode;

        SpannerErrorCode(int code, ErrorType type)
        {
            errorCode = new ErrorCode(code + 0x0506_0000, name(), type);
        }

        @Override
        public ErrorCode toErrorCode()
        {
            return errorCode;
        }
    }
}
