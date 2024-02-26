/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.dynamodb.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.hubspot.jinjava.Jinjava;
import com.starburstdata.trino.plugin.dynamodb.DynamoDbConfig;
import com.starburstdata.trino.plugin.dynamodb.DynamoDbJdbcClient;
import io.trino.spi.TrinoException;
import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class DynamoDbTestTable
        extends TestTable
{
    private final DynamoDbConfig config;
    // TestTable is given a dummy executor, subclasses require their own executor, so the super class needs to be redesigned to avoid this field
    @SuppressWarnings("HidingField")
    private final SqlExecutor sqlExecutor;
    private final List<ColumnSetup> columns;

    public DynamoDbTestTable(DynamoDbConfig config, SqlExecutor sqlExecutor, String namePrefix, List<ColumnSetup> columns)
    {
        // Pass in a no-op SqlExecutor as the base class attempts to create the table, but the driver does not support CREATE TABLE
        super(sql -> {}, namePrefix, "");

        this.config = requireNonNull(config, "config is null");
        this.sqlExecutor = requireNonNull(sqlExecutor, "sqlExecutor is null");
        this.columns = requireNonNull(columns, "columns is null");

        String createTableSql = buildCreateTableSql();
        sqlExecutor.execute(createTableSql);
        generateSchemaFile();

        sqlExecutor.execute("RESET SCHEMA CACHE");
    }

    private String buildCreateTableSql()
    {
        return "EXEC CreateTable TableName = '" + this.getName() + "',\n" +
                "PartitionKeyName = 'col_0',\n" +
                format("PartitionKeyType = '%s',\n", getDynamoDbTypeFromSql(columns.get(0).getDeclaredType().get())) +
                "ReadCapacityUnits = '1',\n" +
                "WriteCapacityUnits = '1'";
    }

    private static String getColumnSize(String declaredType)
    {
        Pattern columnSizePattern = Pattern.compile(".+\\(([0-9]+)\\)");
        Matcher matcher = columnSizePattern.matcher(declaredType);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return null;
    }

    private void generateSchemaFile()
    {
        Jinjava jinjava = new Jinjava();
        Map<String, Object> context = new HashMap<>();
        context.put("tablename", getName());

        List<DynamoDbJdbcClient.RsdColumnDefinition> templateColumns = new ArrayList<>();
        templateColumns.add(new DynamoDbJdbcClient.RsdColumnDefinition(
                "col_0",
                columns.get(0).getDeclaredType().get().contains("varchar") ? "string" : columns.get(0).getDeclaredType().get(),
                true,
                getColumnSize(columns.get(0).getDeclaredType().get()),
                null,
                "HASH",
                getDynamoDbTypeFromSql(columns.get(0).getDeclaredType().get()),
                false));

        int i = 1;
        for (ColumnSetup metadata : columns.subList(1, columns.size())) {
            templateColumns.add(new DynamoDbJdbcClient.RsdColumnDefinition(
                    "col_" + i++,
                    metadata.getDeclaredType().get().contains("varchar") ? "string" : metadata.getDeclaredType().get(),
                    false,
                    getColumnSize(metadata.getDeclaredType().get()),
                    null,
                    null,
                    getDynamoDbTypeFromSql(metadata.getDeclaredType().get()),
                    true));
        }

        context.put("columns", ImmutableList.copyOf(templateColumns.stream().map(DynamoDbJdbcClient.RsdColumnDefinition::asMap).collect(toImmutableList())));

        String template;
        try {
            template = Resources.toString(Resources.getResource("table-schema.tmpl"), StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to find table-schema.tmpl on the classpath");
        }

        String renderedTemplate = jinjava.render(template, context);

        Path outputFile = Paths.get(config.getSchemaDirectory(), getName() + ".rsd");
        try {
            Files.writeString(outputFile, renderedTemplate, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        }
        catch (IOException e) {
            throw new RuntimeException(format("Failed to write schema to file %s", outputFile), e);
        }
    }

    // We use a separate function from what is in DynamoDbJdbcClient to map all specific unsupported types to a DynamoDB String
    // This let's us see what JDBC types the driver does not support
    public static String getDynamoDbTypeFromSql(String type)
    {
        // Map Booleans to Boolean type
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

        return "S";
    }

    @Override
    public void close()
    {
        DynamoDbClientBuilder builder = DynamoDbClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(config.getAwsAccessKey().get(), config.getAwsAccessKey().get())))
                .region(Region.US_EAST_2)
                .endpointOverride(URI.create(config.getEndpointUrl().get()));

        DynamoDbClient client = builder.build();

        client.deleteTable(DeleteTableRequest.builder()
                .tableName(getName())
                .build());

        Path schemaFile = Paths.get(config.getSchemaDirectory(), getName() + ".rsd");
        try {
            Files.deleteIfExists(schemaFile);
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Failed to delete schema file for table %s after dropping", schemaFile.toFile().getAbsolutePath()), e);
        }

        sqlExecutor.execute("RESET SCHEMA CACHE");
    }
}
