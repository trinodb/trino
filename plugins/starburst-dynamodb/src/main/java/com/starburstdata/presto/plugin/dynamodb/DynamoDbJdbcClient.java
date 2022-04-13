/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.dynamodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.hubspot.jinjava.Jinjava;
import com.starburstdata.presto.plugin.jdbc.redirection.TableScanRedirection;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.SliceReadFunction;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.commons.net.util.Base64;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.presto.plugin.dynamodb.DynamoDbTableProperties.getPartitionKeyAttribute;
import static com.starburstdata.presto.plugin.dynamodb.DynamoDbTableProperties.getReadCapacityUnits;
import static com.starburstdata.presto.plugin.dynamodb.DynamoDbTableProperties.getSortKeyAttribute;
import static com.starburstdata.presto.plugin.dynamodb.DynamoDbTableProperties.getWriteCapacityUnits;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.jdbc.ColumnMapping.booleanMapping;
import static io.trino.plugin.jdbc.ColumnMapping.doubleMapping;
import static io.trino.plugin.jdbc.ColumnMapping.longMapping;
import static io.trino.plugin.jdbc.ColumnMapping.sliceMapping;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.jdbc.WriteMapping.sliceMapping;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DynamoDbJdbcClient
        extends BaseJdbcClient
{
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");

    private final TableScanRedirection tableScanRedirection;
    private final File schemaDirectory;
    private final boolean isFirstKeyAsPrimaryKeyEnabled;

    // These properties are needed to drop a table using the AWS SDK. CData driver does not support dropping tables
    private final Optional<String> endpointUrl;
    private final Optional<String> accessKey;
    private final Optional<String> secretAccessKey;
    private final Region region;
    private final boolean enableWrites;

    @Inject
    public DynamoDbJdbcClient(
            BaseJdbcConfig baseJdbcConfig,
            DynamoDbConfig dynamoDbConfig,
            TableScanRedirection tableScanRedirection,
            ConnectionFactory connectionFactory,
            IdentifierMapping identifierMapping,
            QueryBuilder queryBuilder,
            @EnableWrites boolean enableWrites)
    {
        super(baseJdbcConfig, "\"", connectionFactory, queryBuilder, identifierMapping);
        this.tableScanRedirection = requireNonNull(tableScanRedirection, "tableScanRedirection is null");
        this.schemaDirectory = new File(requireNonNull(dynamoDbConfig, "dynamoDbConfig is null").getSchemaDirectory());
        this.isFirstKeyAsPrimaryKeyEnabled = dynamoDbConfig.isFirstColumnAsPrimaryKeyEnabled();
        this.endpointUrl = dynamoDbConfig.getEndpointUrl();
        this.accessKey = dynamoDbConfig.getAwsAccessKey();
        this.secretAccessKey = dynamoDbConfig.getAwsSecretKey();
        this.region = Region.of(dynamoDbConfig.getAwsRegion());
        this.enableWrites = enableWrites;
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> getTableScanRedirection(ConnectorSession session, JdbcTableHandle handle)
    {
        return tableScanRedirection.getTableScanRedirection(session, handle, this);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        if (!enableWrites) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
        }

        try {
            // DynamoDB does not support temporary table names
            return this.createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        if (!enableWrites) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
        }

        // no-op
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        if (!enableWrites) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
        }

        // We are directly writing to the target table so this is a no-op
    }

    @Override
    protected JdbcOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, String tableName)
            throws SQLException
    {
        if (!enableWrites) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
        }

        Map<String, Object> tableProperties = tableMetadata.getProperties();

        StringBuilder sql = new StringBuilder();
        sql.append(format("EXEC CreateTable TableName = '%s', ", tableName));

        String partitionKeyAttribute = getPartitionKeyAttribute(tableProperties).orElseGet(() -> {
            if (isFirstKeyAsPrimaryKeyEnabled) {
                return tableMetadata.getColumns().get(0).getName();
            }
            else {
                throw new TrinoException(GENERIC_USER_ERROR, "partition_key_attribute is not specified and dynamodb.first-key-as-primary-key-enabled is false");
            }
        });

        Optional<ColumnMetadata> partitionKeyMetadata = tableMetadata.getColumns().stream().filter(column -> column.getName().equalsIgnoreCase(partitionKeyAttribute)).findFirst();
        if (partitionKeyMetadata.isEmpty()) {
            throw new TrinoException(GENERIC_USER_ERROR, format("Attribute %s specified for primary key but is not present in the column list", partitionKeyAttribute));
        }

        sql.append(format("PartitionKeyName = '%s', PartitionKeyType = '%s', ", partitionKeyAttribute, getDynamoDbTypeFromSql(partitionKeyMetadata.get().getType().getDisplayName())));

        Optional<String> sortKeyAttribute = getSortKeyAttribute(tableProperties);
        Optional<ColumnMetadata> sortKeyMetadata = sortKeyAttribute.flatMap(s -> tableMetadata.getColumns().stream().filter(column -> column.getName().equalsIgnoreCase(s)).findFirst());

        sortKeyAttribute.ifPresent(attribute -> {
            if (partitionKeyAttribute.equalsIgnoreCase(attribute)) {
                throw new TrinoException(GENERIC_USER_ERROR, "Partition key and sort key cannot be the same attribute");
            }

            if (sortKeyMetadata.isEmpty()) {
                throw new TrinoException(GENERIC_USER_ERROR, format("Attribute %s specified for sort key but is not present in the column list", attribute));
            }

            sql.append(format("SortKeyName = '%s', SortKeyType = '%s', ", attribute, getDynamoDbTypeFromSql(sortKeyMetadata.get().getType().getDisplayName())));
        });

        sql.append(format("ProvisionedThroughput_ReadCapacityUnits = '%s', ProvisionedThroughput_WriteCapacityUnits = '%s'",
                getReadCapacityUnits(tableProperties),
                getWriteCapacityUnits(tableProperties)));

        try (Connection connection = connectionFactory.openConnection(session);
                Statement statement = connection.createStatement()) {
            statement.execute(sql.toString());
        }

        generateSchemaFile(tableName, partitionKeyMetadata.get(), sortKeyMetadata, tableMetadata.getColumns());

        invalidateDriverCache(session);

        return new JdbcOutputTableHandle("dynamodb",
                "amazondynamodb",
                tableName,
                tableMetadata.getColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toList()),
                tableMetadata.getColumns().stream().map(ColumnMetadata::getType).collect(Collectors.toList()),
                Optional.empty(),
                tableName);
    }

    @Override
    public List<SchemaTableName> getTableNames(ConnectorSession session, Optional<String> schema)
    {
        try {
            return super.getTableNames(session, schema);
        }
        catch (TrinoException e) {
            // CData driver throws an exception if there are no tables in DynamoDB
            if (e.getMessage().contains("No tables found. Please check your connection settings.")) {
                return ImmutableList.of();
            }

            throw e;
        }
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
    {
        // Don't return a comment until the connector supports creating tables with comment
        return Optional.empty();
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        try {
            return super.getTableHandle(session, schemaTableName);
        }
        catch (TrinoException exception) {
            // CData driver throws an exception if there are no tables in DynamoDB
            if (exception.getMessage().contains("No tables found. Please check your connection settings.")) {
                return Optional.empty();
            }

            throw exception;
        }
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle handle)
    {
        if (!enableWrites) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping tables");
        }

        checkState(accessKey.isPresent(), "access key is not set but it is required for dropping tables");
        checkState(secretAccessKey.isPresent(), "secret key is not set but it is required for dropping tables");

        DynamoDbClientBuilder builder = DynamoDbClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey.get(), secretAccessKey.get())))
                .region(region);

        endpointUrl.ifPresent(url -> builder.endpointOverride(URI.create(url)));

        DynamoDbClient client = builder.build();

        String tableName = handle.asPlainTable().getRemoteTableName().getTableName();
        client.deleteTable(DeleteTableRequest.builder()
                .tableName(tableName)
                .build());

        invalidateDriverCache(session);

        Path schemaFile = Paths.get(schemaDirectory.getAbsolutePath(), tableName + ".rsd");
        try {
            Files.deleteIfExists(schemaFile);
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Failed to delete schema file for table %s after dropping. The schema file needs to be deleted otherwise the table will still appear as existing even though it was actually dropped.", schemaFile.toFile().getAbsolutePath()), e);
        }
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns");
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        if (!enableWrites) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support inserts");
        }

        SchemaTableName schemaTableName = tableHandle.asPlainTable().getSchemaTableName();

        try (Connection connection = connectionFactory.openConnection(session)) {
            String remoteSchema = getIdentifierMapping().toRemoteSchemaName(session.getIdentity(), connection, schemaTableName.getSchemaName());
            String remoteTable = getIdentifierMapping().toRemoteTableName(session.getIdentity(), connection, remoteSchema, schemaTableName.getTableName());

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<JdbcTypeHandle> jdbcColumnTypes = ImmutableList.builder();
            for (JdbcColumnHandle column : columns) {
                columnNames.add(column.getColumnName());
                columnTypes.add(column.getColumnType());
                jdbcColumnTypes.add(column.getJdbcTypeHandle());
            }

            return new JdbcOutputTableHandle(
                    connection.getCatalog(),
                    remoteSchema,
                    remoteTable,
                    columnNames.build(),
                    columnTypes.build(),
                    Optional.of(jdbcColumnTypes.build()),
                    schemaTableName.getTableName());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        if (!enableWrites) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support inserts");
        }

        // no-op, data is inserted into real table
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncating tables");
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        // CData driver returns predicates where you compare to null as 'true', e.g. value != 123 is true when "value" is null
        // Trino treats this as false and causes correctness issues when these predicates are pushed down to the driver
        // Therefore all pushdowns are disabled
        switch (typeHandle.getJdbcType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                // Error if pushdown is enabled (besides the null issue):
                // Invalid FilterExpression: Incorrect operand type for operator or function; operator or function: <=, operand type: BOOL.
                return Optional.of(booleanMapping(BOOLEAN, ResultSet::getBoolean, booleanWriteFunction(), DISABLE_PUSHDOWN));
            case Types.TINYINT:
                return Optional.of(longMapping(TINYINT, ResultSet::getByte, tinyintWriteFunction(), DISABLE_PUSHDOWN));
            case Types.SMALLINT:
                return Optional.of(longMapping(SMALLINT, ResultSet::getShort, smallintWriteFunction(), DISABLE_PUSHDOWN));
            case Types.INTEGER:
                return Optional.of(longMapping(INTEGER, ResultSet::getInt, integerWriteFunction(), DISABLE_PUSHDOWN));
            case Types.BIGINT:
                return Optional.of(longMapping(BIGINT, ResultSet::getLong, bigintWriteFunction(), DISABLE_PUSHDOWN));
            case Types.REAL:
            case Types.FLOAT:
                return Optional.of(longMapping(REAL, (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)), realWriteFunction(), DISABLE_PUSHDOWN));
            case Types.DOUBLE:
                return Optional.of(doubleMapping(DOUBLE, ResultSet::getDouble, doubleWriteFunction(), DISABLE_PUSHDOWN));
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize()));
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(sliceMapping(VARBINARY, varbinaryReadFunction(), varbinaryWriteFunction(), DISABLE_PUSHDOWN));
            case Types.DATE:
                return Optional.of(longMapping(DATE, dateReadFunctionUsingString(), dateWriteFunctionUsingLocalDate(), DISABLE_PUSHDOWN));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    public static ColumnMapping defaultVarcharColumnMapping(int columnSize)
    {
        if (columnSize > VarcharType.MAX_LENGTH) {
            return varcharColumnMapping(createUnboundedVarcharType());
        }
        return varcharColumnMapping(createVarcharType(columnSize));
    }

    public static ColumnMapping varcharColumnMapping(VarcharType varcharType)
    {
        return ColumnMapping.sliceMapping(varcharType, varcharReadFunction(varcharType), varcharWriteFunction(), DISABLE_PUSHDOWN);
    }

    public static SliceReadFunction varbinaryReadFunction()
    {
        return (resultSet, columnIndex) -> wrappedBuffer(resultSet.getBytes(columnIndex));
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type.equals(BOOLEAN)) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }

        if (type.equals(TINYINT)) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }

        if (type.equals(SMALLINT)) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }

        if (type.equals(INTEGER)) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }

        if (type.equals(BIGINT)) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }

        if (type.equals(REAL)) {
            return WriteMapping.longMapping("real", realWriteFunction());
        }

        if (type.equals(DOUBLE)) {
            return WriteMapping.doubleMapping("double", doubleWriteFunction());
        }

        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            // The CData driver returns varchar(2000) for unbounded varchar types, so refuse to create it
            if (varcharType.isUnbounded()) {
                throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName() + ". Only bounded varchars are supported");
            }

            return sliceMapping(format("varchar(%s)", varcharType.getBoundedLength()), varcharWriteFunction());
        }

        if (type instanceof VarbinaryType) {
            return sliceMapping("varbinary", varbinaryWriteFunction());
        }

        if (type.equals(DATE)) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    public static SliceWriteFunction varbinaryWriteFunction()
    {
        return (statement, index, value) -> statement.setString(index, Base64.encodeBase64String(value.getBytes()));
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        // This function is overridden to ensure we have a class in the com.starburstdata.* package when running queries
        // Without it, the query would fail with a CData licensing error
        return super.getPreparedStatement(connection, sql);
    }

    public static String getDynamoDbTypeFromSql(String type)
    {
        if (type.equals("boolean")) {
            return "BOOL";
        }

        if (type.contains("varbinary")) {
            return "B";
        }

        // Map numeric types
        if (ImmutableSet.of("tinyint", "smallint", "bigint", "integer", "real", "double").contains(type)) {
            return "N";
        }

        if (type.equals("varchar")) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type + ". Only bounded varchars are supported");
        }

        if (type.contains("varchar") || type.equals("date")) {
            return "S";
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    private void generateSchemaFile(String tableName, ColumnMetadata partitionKeyMetadata, Optional<ColumnMetadata> sortKeyMetadata, List<ColumnMetadata> columns)
    {
        Jinjava jinjava = new Jinjava();
        Map<String, Object> context = new HashMap<>();
        context.put("tablename", tableName);

        List<RsdColumnDefinition> templateColumns = new ArrayList<>();
        templateColumns.add(new RsdColumnDefinition(
                partitionKeyMetadata.getName(),
                partitionKeyMetadata.getType() instanceof VarcharType ? "string" : partitionKeyMetadata.getType().getTypeSignature().toString(),
                true,
                getColumnSize(partitionKeyMetadata),
                partitionKeyMetadata.getComment(),
                "HASH",
                getDynamoDbTypeFromSql(partitionKeyMetadata.getType().getDisplayName())));

        sortKeyMetadata.ifPresent(metadata ->
                templateColumns.add(new RsdColumnDefinition(
                        sortKeyMetadata.get().getName(),
                        sortKeyMetadata.get().getType() instanceof VarcharType ? "string" : sortKeyMetadata.get().getType().getTypeSignature().toString(),
                        true,
                        getColumnSize(sortKeyMetadata.get()),
                        sortKeyMetadata.get().getComment(),
                        "RANGE",
                        getDynamoDbTypeFromSql(sortKeyMetadata.get().getType().getDisplayName()))));

        for (ColumnMetadata metadata : columns) {
            if (metadata.getName().equalsIgnoreCase(partitionKeyMetadata.getName()) ||
                    (sortKeyMetadata.isPresent() && metadata.getName().equalsIgnoreCase(sortKeyMetadata.get().getName()))) {
                continue;
            }

            templateColumns.add(new RsdColumnDefinition(
                    metadata.getName(),
                    metadata.getType() instanceof VarcharType ? "string" : metadata.getType().getTypeSignature().toString(),
                    false,
                    getColumnSize(metadata),
                    metadata.getComment(),
                    null,
                    getDynamoDbTypeFromSql(metadata.getType().getDisplayName())));
        }

        context.put("columns", ImmutableList.copyOf(templateColumns.stream().map(RsdColumnDefinition::asMap).collect(toImmutableList())));

        String template;
        try {
            template = Resources.toString(Resources.getResource("table-schema.tmpl"), StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to find table-schema.tmpl on the classpath");
        }

        String renderedTemplate = jinjava.render(template, context);

        Path outputFile = Paths.get(schemaDirectory.getAbsolutePath(), tableName + ".rsd");
        try {
            Files.writeString(outputFile, renderedTemplate, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        }
        catch (IOException e) {
            throw new RuntimeException(format("Failed to write schema to file %s", outputFile), e);
        }
    }

    private static String getColumnSize(ColumnMetadata columnMetadata)
    {
        if (columnMetadata.getType() instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) columnMetadata.getType();
            if (!varcharType.isUnbounded()) {
                return Integer.toString(varcharType.getBoundedLength());
            }
        }
        return null;
    }

    private void invalidateDriverCache(ConnectorSession session)
    {
        // Reset CData metadata cache after creating a table so it will be visible
        try (Connection connection = connectionFactory.openConnection(session);
                Statement statement = connection.createStatement()) {
            statement.execute("RESET SCHEMA CACHE");
        }
        catch (SQLException throwables) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to reset metadata cache after create table operation");
        }
    }

    private static LongReadFunction dateReadFunctionUsingString()
    {
        return new LongReadFunction()
        {
            @Override
            public boolean isNull(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                resultSet.getString(columnIndex);
                return resultSet.wasNull();
            }

            @Override
            public long readLong(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                return LocalDate.parse(resultSet.getString(columnIndex), DATE_FORMATTER).toEpochDay();
            }
        };
    }

    private static LongWriteFunction dateWriteFunctionUsingLocalDate()
    {
        return (statement, index, value) -> statement.setObject(index, LocalDate.ofEpochDay(value));
    }

    public static class RsdColumnDefinition
    {
        String name;
        String type;
        boolean isKey;
        String columnSize;
        boolean readOnly;
        String description;
        String keyType;
        String path;
        String internalType;

        public RsdColumnDefinition(String name, String type, boolean isKey, String columnSize, String description, String keyType, String internalType)
        {
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
            this.isKey = isKey;
            this.columnSize = columnSize;
            this.readOnly = false;
            this.description = description;
            this.keyType = keyType;
            this.path = name;
            this.internalType = requireNonNull(internalType, "internalType is null");
        }

        public Map<String, Object> asMap()
        {
            Map<String, Object> values = new HashMap<>();
            values.put("name", name);
            values.put("type", type);
            values.put("isKey", isKey);
            values.put("columnSize", columnSize);
            values.put("readOnly", readOnly);
            values.put("description", description);
            values.put("keyType", keyType);
            values.put("path", path);
            values.put("internalType", internalType);
            return values;
        }
    }
}
