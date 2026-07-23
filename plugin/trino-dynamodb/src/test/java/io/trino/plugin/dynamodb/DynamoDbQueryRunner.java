/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.dynamodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchColumnType;
import io.trino.tpch.TpchEntity;
import io.trino.tpch.TpchTable;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class DynamoDbQueryRunner
{
    private DynamoDbQueryRunner() {}

    private static final double TINY_SCALE_FACTOR = 0.01;
    private static final String CATALOG = "dynamodb";
    private static final String SCHEMA = "default";

    public static Builder builder(DynamoDbServer server)
    {
        return new Builder(server);
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final DynamoDbServer server;
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder(DynamoDbServer server)
        {
            super(testSessionBuilder()
                    .setCatalog(CATALOG)
                    .setSchema(SCHEMA)
                    .build());
            this.server = requireNonNull(server, "server is null");
        }

        @CanIgnoreReturnValue
        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new DynamoDbPlugin());
                queryRunner.createCatalog(CATALOG, "dynamodb", ImmutableMap.<String, String>builder()
                        .put("dynamodb.aws-access-key", "test")
                        .put("dynamodb.aws-secret-key", "test")
                        .put("dynamodb.aws-region", "us-east-1")
                        .put("dynamodb.endpoint-override", server.getEndpointUrl().toString())
                        .buildOrThrow());

                try (DynamoDbClient client = server.createClient()) {
                    for (TpchTable<?> table : initialTables) {
                        loadTpchTable(client, table);
                    }
                }

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    private static <E extends TpchEntity> void loadTpchTable(DynamoDbClient client, TpchTable<E> table)
    {
        String tableName = table.getTableName();
        TpchColumn<E> hashKeyColumn = table.getColumns().get(0);
        ScalarAttributeType keyType = tpchColumnToDynamoDbType(hashKeyColumn);

        client.createTable(CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(KeySchemaElement.builder()
                        .attributeName(hashKeyColumn.getSimplifiedColumnName())
                        .keyType(KeyType.HASH)
                        .build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName(hashKeyColumn.getSimplifiedColumnName())
                        .attributeType(keyType)
                        .build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());

        waitForTableActive(client, tableName);

        List<E> items = new ArrayList<>();
        for (E entity : table.createGenerator(TINY_SCALE_FACTOR, 1, 1)) {
            items.add(entity);
        }

        for (int i = 0; i < items.size(); i += 25) {
            List<WriteRequest> batch = new ArrayList<>();
            for (E entity : items.subList(i, Math.min(i + 25, items.size()))) {
                batch.add(WriteRequest.builder()
                        .putRequest(PutRequest.builder()
                                .item(tpchEntityToItem(table, entity))
                                .build())
                        .build());
            }
            client.batchWriteItem(r -> r.requestItems(Map.of(tableName, batch)));
        }
    }

    private static <E extends TpchEntity> Map<String, AttributeValue> tpchEntityToItem(TpchTable<E> table, E entity)
    {
        Map<String, AttributeValue> item = new HashMap<>();
        for (TpchColumn<E> column : table.getColumns()) {
            String name = column.getSimplifiedColumnName();
            AttributeValue value = tpchColumnToAttributeValue(column, entity);
            if (value != null) {
                item.put(name, value);
            }
        }
        return item;
    }

    private static <E extends TpchEntity> AttributeValue tpchColumnToAttributeValue(TpchColumn<E> column, E entity)
    {
        TpchColumnType.Base base = column.getType().getBase();
        return switch (base) {
            case IDENTIFIER -> AttributeValue.fromN(String.valueOf(column.getIdentifier(entity)));
            case INTEGER -> AttributeValue.fromN(String.valueOf(column.getInteger(entity)));
            case DOUBLE -> AttributeValue.fromN(String.valueOf(column.getDouble(entity)));
            case DATE -> AttributeValue.fromS(column.getString(entity));
            case VARCHAR -> AttributeValue.fromS(column.getString(entity));
        };
    }

    private static <E extends TpchEntity> ScalarAttributeType tpchColumnToDynamoDbType(TpchColumn<E> column)
    {
        return switch (column.getType().getBase()) {
            case IDENTIFIER, INTEGER, DOUBLE -> ScalarAttributeType.N;
            case VARCHAR, DATE -> ScalarAttributeType.S;
        };
    }

    private static void waitForTableActive(DynamoDbClient client, String tableName)
    {
        int maxAttempts = 20;
        for (int i = 0; i < maxAttempts; i++) {
            TableStatus status = client.describeTable(r -> r.tableName(tableName))
                    .table()
                    .tableStatus();
            if (status == TableStatus.ACTIVE) {
                return;
            }
            try {
                Thread.sleep(200);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        throw new RuntimeException("Table %s did not become ACTIVE in time".formatted(tableName));
    }

    static void main()
            throws Exception
    {
        DynamoDbServer server = new DynamoDbServer();
        QueryRunner queryRunner = builder(server)
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setInitialTables(List.of(TpchTable.NATION, TpchTable.REGION, TpchTable.ORDERS, TpchTable.CUSTOMER))
                .build();
        System.out.println("======== SERVER STARTED ========");
        System.out.println(queryRunner.getCoordinator().getBaseUrl());
    }
}
