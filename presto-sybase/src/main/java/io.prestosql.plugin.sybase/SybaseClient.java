package io.prestosql.plugin.sybase;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.*;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.Type;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TypeParameter.of;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.isNull;


public class SybaseClient
        extends BaseJdbcClient {

    private final Map<String, LoadingCache<String, Optional<String>>> schemaTableMapping = new ConcurrentHashMap<>();
    private static final Logger log = Logger.get(SybaseClient.class);
    private static final Map<Type, String> SQL_TYPES = ImmutableMap.<Type, String>builder()
            .put(BOOLEAN, "boolean")
            .put(BIGINT, "bigint")
            .put(INTEGER, "integer")
            .put(SMALLINT, "smallint")
            .put(TINYINT, "tinyint")
            .put(DOUBLE, "double precision")
            .put(REAL, "real")
            .put(VARBINARY, "varbinary")
            .put(DATE, "date")
            .put(TIME, "time")
            .put(TIME_WITH_TIME_ZONE, "time with time zone")
            .put(TIMESTAMP, "timestamp")
            .put(VARCHAR, "varchar")
            .build();

    @Inject
    public SybaseClient(BaseJdbcConfig config, ConnectionFactory connectionFactory) {
        super(config, "\"", connectionFactory);
    }

    @Override
    public boolean isLimitGuaranteed() {
        return true;
    }

    /**
     * Pull the list of Table names from the RDBMS exactly as they are returned. Override if the RDBMS method to pull the tables is different. Each {
     * the table’s original schema name as the first element, and the original table name on the second element
     *
     * @param schema the schema to the list the tables for, or NULL for all
     * @return the schema + table names
     */

    protected List<CaseSensitiveMappedSchemaTableName> getOriginalTablesWithSchema(Connection connection, String schema) {
        try (ResultSet resultSet = getTables(connection, Optional.of(schema), Optional.empty())) {
            ImmutableList.Builder<CaseSensitiveMappedSchemaTableName> list = ImmutableList.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE SCHEM");
                String tablename = resultSet.getString("TABLE_NAME");
                list.add(new CaseSensitiveMappedSchemaTableName(schemaName, tablename));
            }
            return list.build();
        } catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public List<SchemaTableName> getTableNames(JdbcIdentity identity, Optional<String> schema) {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            DatabaseMetaData metadata = connection.getMetaData();

            String finalizeSchema = finalizeSchemaName(metadata, schema.get());
            Map<String, Map<String, String>> schemaMappedNames = new HashMap<>();
            ImmutableList.Builder<SchemaTableName> tableNameList = ImmutableList.builder();
            for (CaseSensitiveMappedSchemaTableName table : getOriginalTablesWithSchema(connection, finalizeSchema)) {
                Map<String, String> mappedNames = schemaMappedNames.computeIfAbsent(table.getSchemaName(), s -> new HashMap<>());
                mappedNames.put(table.getSchemaNameLower(), table.getTableName());
                tableNameList.add(new SchemaTableName(table.getSchemaNameLower(), table.getTableNameLower()));
                // if someone is listing all of the table names, throw them all into the cache as a refresh since we already spent the time pulling
                for (Map.Entry<String, Map<String, String>> entry : schemaMappedNames.entrySet()) {
                    updateTableMapping(identity, entry.getKey(), entry.getValue());
                }
            }
            return tableNameList.build();
        } catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected Collection<String> listSchemas(Connection connection) {
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE SCHEM");
                // skip internal schemas
                if (!schemaName.equalsIgnoreCase("information schema")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        } catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private void updateTableMapping(JdbcIdentity identity, String jdbcSchema, Map<String, String> tableMapping) {
        LoadingCache<String, Optional<String>> tableNameMapping = getTableMapping(identity, jdbcSchema);
        Map<String, Optional<String>> tmp = new HashMap<>();
        for (Map.Entry<String, String> entry : tableMapping.entrySet()) {
            tmp.put(entry.getKey(), Optional.of(entry.getValue()));
        }
        tableNameMapping.putAll(tmp);
    }

    /**
     * Fetch the {@link LoadingCache} of table mapped names for the given schema name
     *
     * @param jdbcSchema the original name of the schema as it exists in the JDBC server
     * @return the {@link LoadingCache} which contains the table mapped names
     */

    private LoadingCache<String, Optional<String>> getTableMapping(JdbcIdentity identity, String jdbcSchema) {
        return schemaTableMapping.computeIfAbsent(jdbcSchema, (String s) ->
                CacheBuilder.newBuilder().build(new CacheLoader<String, Optional<String>>() {
                    @Override
                    public Optional<String> load(String key) throws Exception {
                        try (Connection connection = connectionFactory.openConnection(identity)) {
                            DatabaseMetaData metadata = connection.getMetaData();
                            String jdbcSchemaName = finalizeSchemaName(metadata, jdbcSchema);

                            for (CaseSensitiveMappedSchemaTableName table : getOriginalTablesWithSchema(connection, jdbcSchemaName)) {
                                String tableName = table.getTableName();
                                if (tableName.equals(key)) {
                                    return Optional.of(tableName);
                                }

                                String tableNameLower = table.getTableNameLower();
                                if (tableNameLower.equals(key)) {
                                    return Optional.of(tableName);
                                }

                            }
                        } catch (SQLException e) {
                            throw new PrestoException(JDBC_ERROR, e);
                        }
                        return Optional.empty();
                    }
                }));
    }

    /**
     * Looks up the table name given to map it to it’s original, case-sensitive name in the database
     *
     * @param jdbcSchema The database schema name
     * @param jdbcSchema The database schema name
     * @param tableName The table name within the schema that needs to be mapped to it’s original, or NULL if it couldn’t be found in the cache
     * @return The mapped case sensitive table name, if found, otherwise the original table name passed in
     */
    private String getMappedTableName(JdbcIdentity identity, String jdbcSchema, String tableName) {
        LoadingCache<String, Optional<String>> tableNameMapping = getTableMapping(identity, jdbcSchema);
        Optional<String> value = tableNameMapping.getUnchecked(tableName);
        if (value.isPresent()) {
            return value.get();
        }

        tableNameMapping.invalidate(jdbcSchema);
        return null;
    }

    protected String finalizeSchemaName(DatabaseMetaData metadata, String schemaName)
            throws SQLException {
        if (schemaName == null) {
            return null;
        }
        if (metadata.storesUpperCaseIdentifiers()) {
            return schemaName.toUpperCase(ENGLISH);
        } else {
            Optional<String> value = Optional.ofNullable(schemaName);
            //schemaMappingCache,getUnchecked(schemaName)
            if (value.isPresent()) {
                return value.get();
            }
            // If we get back an Empty value,invalidate it now so that in the future we can try and load it again into cache and return the value
            //At this point it is extremely likely we are trying to create the schema,since it wasn’t healthy initially present and couldn’t be found current schemaMappingcache.invalidate(schemaName);
            return schemaName;
        }
    }

    protected String finalizeTableName(JdbcIdentity identity, DatabaseMetaData metaData, String schemaName, String tableName) throws SQLException {
        if (schemaName == null || tableName == null) {
            return null;
        }

        if (metaData.storesUpperCaseIdentifiers()) {
            return tableName.toUpperCase(ENGLISH);
        } else {
            String value = getMappedTableName(identity, schemaName, tableName);
            if (value == null) {
                return tableName;
            }
            // if we couldn’t look up the table in the mapping cache, return the value we were given return value;
            return value;
        }
    }


    @Nullable
    @Override
    public Optional<JdbcTableHandle> getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName) {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            DatabaseMetaData metadata = connection.getMetaData();
            String jdbcSchemaName = finalizeSchemaName(metadata, schemaTableName.getSchemaName());
            String jdbcTableName = finalizeTableName(identity, metadata, jdbcSchemaName, schemaTableName.getTableName());
            if (jdbcTableName == null) {
                return null;
            }
            try (ResultSet resultSet = getTables(connection, Optional.of(jdbcSchemaName), Optional.of(jdbcTableName))) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(
                            schemaTableName,
                            resultSet.getString("TABLE CAT"),
                            resultSet.getString("TABLE SCHEM"),
                            resultSet.getString("TABLE NAME")));
                }
                if (tableHandles.isEmpty()) {
                    return null;
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched;" + schemaTableName);

                }
                return Optional.of(getOnlyElement(tableHandles));
            }
        } catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                new String[]{"TABLE", "VIEW", "MATERIALIZED VIEW", "FORGIEN TABLE"});
    }

    private SybaseTableDescription loadTable(String tableDescriptionPath) {
        Map<String, String> fileInfo = new HashMap<>();
        try {
            File file = new File(tableDescriptionPath);
            BufferedReader br = new BufferedReader(new FileReader(file));
            String st;
            while ((st = br.readLine()) != null) {
                String[] pairs = st.split(" *= *", 2);
                fileInfo.put(pairs[0], pairs.length == 1 ? "" : pairs[1]);
            }
            return new SybaseTableDescription(
                    fileInfo.get("schemaName"),
                    fileInfo.get("tableName"),
                    fileInfo.get("column"),
                    !isNull(fileInfo.get("minValue")) ? NumberFormat.getInstance().parse(fileInfo.get("minValue")) : null,
                    !isNull(fileInfo.get("maxValue")) ? NumberFormat.getInstance().parse(fileInfo.get("maxValue")) : null,
                    Integer.valueOf(fileInfo.get("numberPartitions"))
            );

        } catch (ParseException | IOException e) {
            log.error("File path is invalid!!!");
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void commitCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle) {
    // SybaseIQ doesnot allow qualifying the target of a rename
        StringBuilder sql = new StringBuilder()
                .append("ALTER TABLE")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()))
                .append(" RENAME TO ")
                .append(quoted(handle.getTableName()));

        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(0);
        return statement;
    }


}
