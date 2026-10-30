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
package io.trino.metastore.lakehouse;

import com.google.cloud.biglake.hive.v1.GetHiveDatabaseRequest;
import com.google.cloud.biglake.hive.v1.GetHiveTableRequest;
import com.google.cloud.biglake.hive.v1.HiveDatabase;
import com.google.cloud.biglake.hive.v1.HiveMetastoreServiceGrpc;
import com.google.cloud.biglake.hive.v1.HiveTable;
import com.google.cloud.biglake.hive.v1.ListHiveDatabasesRequest;
import com.google.cloud.biglake.hive.v1.ListHiveDatabasesResponse;
import com.google.cloud.biglake.hive.v1.ListHiveTablesRequest;
import com.google.cloud.biglake.hive.v1.ListHiveTablesResponse;
import com.google.cloud.biglake.hive.v1.ListPartitionsRequest;
import com.google.cloud.biglake.hive.v1.ListPartitionsResponse;
import com.google.cloud.biglake.hive.v1.NamespaceName;
import com.google.cloud.biglake.hive.v1.Partition;
import com.google.cloud.biglake.hive.v1.TableName;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Math.min;

/**
 * A hand-written Lakehouse Hive service that answers from memory and records what it was asked.
 *
 * <p>It answers like the real service where the client depends on it: a missing project, database
 * or table is {@code NOT_FOUND}, a missing catalog, and a database that is missing from a request
 * for its tables or partitions, is {@code FAILED_PRECONDITION}, a listing is split into pages, and
 * the partitions of a table arrive as a stream of responses.
 */
public final class TestingHiveMetastoreService
        extends HiveMetastoreServiceGrpc.HiveMetastoreServiceImplBase
{
    private final List<HiveDatabase> databases = new ArrayList<>();
    private final Map<String, List<HiveTable>> tablesByDatabase = new LinkedHashMap<>();
    private final Map<String, List<Partition>> partitionsByTable = new LinkedHashMap<>();
    private final List<String> requests = new ArrayList<>();
    private final List<String> partitionFilters = new ArrayList<>();
    private boolean projectExists = true;
    private boolean catalogExists = true;
    private int pageSize = Integer.MAX_VALUE;
    private int partitionsPerResponse = Integer.MAX_VALUE;
    private Status failure;

    public TestingHiveMetastoreService add(HiveDatabase database)
    {
        databases.add(database);
        tablesByDatabase.putIfAbsent(database.getName(), new ArrayList<>());
        return this;
    }

    public TestingHiveMetastoreService add(HiveDatabase database, HiveTable table)
    {
        add(database);
        tablesByDatabase.get(database.getName()).add(table);
        partitionsByTable.putIfAbsent(table.getName(), new ArrayList<>());
        return this;
    }

    public TestingHiveMetastoreService add(HiveTable table, Partition partition)
    {
        partitionsByTable.computeIfAbsent(table.getName(), _ -> new ArrayList<>()).add(partition);
        return this;
    }

    public TestingHiveMetastoreService withoutProject()
    {
        projectExists = false;
        return this;
    }

    public TestingHiveMetastoreService withoutCatalog()
    {
        catalogExists = false;
        return this;
    }

    public TestingHiveMetastoreService withPageSize(int pageSize)
    {
        this.pageSize = pageSize;
        return this;
    }

    public TestingHiveMetastoreService withPartitionsPerResponse(int partitionsPerResponse)
    {
        this.partitionsPerResponse = partitionsPerResponse;
        return this;
    }

    public TestingHiveMetastoreService failWith(Status failure)
    {
        this.failure = failure;
        return this;
    }

    public List<String> requests()
    {
        return List.copyOf(requests);
    }

    public List<String> partitionFilters()
    {
        return List.copyOf(partitionFilters);
    }

    @Override
    public void listHiveDatabases(ListHiveDatabasesRequest request, StreamObserver<ListHiveDatabasesResponse> observer)
    {
        requests.add("listHiveDatabases " + request.getParent());
        if (failed(observer) || parentMissing(observer)) {
            return;
        }
        int start = request.getPageToken().isEmpty() ? 0 : Integer.parseInt(request.getPageToken());
        int end = min(databases.size(), start + pageSize);
        ListHiveDatabasesResponse.Builder response = ListHiveDatabasesResponse.newBuilder()
                .addAllDatabases(databases.subList(start, end));
        if (end < databases.size()) {
            response.setNextPageToken(String.valueOf(end));
        }
        observer.onNext(response.build());
        observer.onCompleted();
    }

    @Override
    public void getHiveDatabase(GetHiveDatabaseRequest request, StreamObserver<HiveDatabase> observer)
    {
        requests.add("getHiveDatabase " + request.getName());
        if (failed(observer) || parentMissing(observer)) {
            return;
        }
        databases.stream()
                .filter(database -> database.getName().equals(request.getName()))
                .findFirst()
                .ifPresentOrElse(
                        database -> {
                            observer.onNext(database);
                            observer.onCompleted();
                        },
                        () -> notFound(observer));
    }

    @Override
    public void listHiveTables(ListHiveTablesRequest request, StreamObserver<ListHiveTablesResponse> observer)
    {
        requests.add("listHiveTables " + request.getParent());
        if (failed(observer) || parentMissing(observer)) {
            return;
        }
        List<HiveTable> tables = tablesByDatabase.get(request.getParent());
        if (tables == null) {
            databaseMissing(observer, request.getParent());
            return;
        }
        int start = request.getPageToken().isEmpty() ? 0 : Integer.parseInt(request.getPageToken());
        int end = min(tables.size(), start + pageSize);
        ListHiveTablesResponse.Builder response = ListHiveTablesResponse.newBuilder()
                .addAllTables(tables.subList(start, end));
        if (end < tables.size()) {
            response.setNextPageToken(String.valueOf(end));
        }
        observer.onNext(response.build());
        observer.onCompleted();
    }

    @Override
    public void getHiveTable(GetHiveTableRequest request, StreamObserver<HiveTable> observer)
    {
        requests.add("getHiveTable " + request.getName());
        if (failed(observer) || parentMissing(observer)) {
            return;
        }
        if (!tablesByDatabase.containsKey(databaseOf(request.getName()))) {
            databaseMissing(observer, databaseOf(request.getName()));
            return;
        }
        tablesByDatabase.values().stream()
                .flatMap(List::stream)
                .filter(table -> table.getName().equals(request.getName()))
                .findFirst()
                .ifPresentOrElse(
                        table -> {
                            observer.onNext(table);
                            observer.onCompleted();
                        },
                        () -> notFound(observer));
    }

    @Override
    public void listPartitions(ListPartitionsRequest request, StreamObserver<ListPartitionsResponse> observer)
    {
        requests.add("listPartitions " + request.getParent());
        partitionFilters.add(request.getFilter());
        if (failed(observer) || parentMissing(observer)) {
            return;
        }
        if (!tablesByDatabase.containsKey(databaseOf(request.getParent()))) {
            databaseMissing(observer, databaseOf(request.getParent()));
            return;
        }
        List<Partition> partitions = partitionsByTable.get(request.getParent());
        if (partitions == null) {
            notFound(observer);
            return;
        }
        for (int start = 0; start < partitions.size(); start += partitionsPerResponse) {
            int end = min(partitions.size(), start + partitionsPerResponse);
            observer.onNext(ListPartitionsResponse.newBuilder()
                    .addAllPartitions(partitions.subList(start, end))
                    .build());
        }
        observer.onCompleted();
    }

    private boolean failed(StreamObserver<?> observer)
    {
        if (failure == null) {
            return false;
        }
        observer.onError(failure.asRuntimeException());
        return true;
    }

    private boolean parentMissing(StreamObserver<?> observer)
    {
        if (!projectExists) {
            notFound(observer);
            return true;
        }
        if (!catalogExists) {
            observer.onError(Status.FAILED_PRECONDITION.withDescription("Hive catalog does not exist.").asRuntimeException());
            return true;
        }
        return false;
    }

    private static void databaseMissing(StreamObserver<?> observer, String database)
    {
        observer.onError(Status.FAILED_PRECONDITION.withDescription("Hive database " + database + " does not exist.").asRuntimeException());
    }

    private static String databaseOf(String tableName)
    {
        TableName table = TableName.parse(tableName);
        return NamespaceName.format(table.getProject(), table.getCatalog(), table.getDatabase());
    }

    private static void notFound(StreamObserver<?> observer)
    {
        observer.onError(Status.NOT_FOUND.withDescription("not found").asRuntimeException());
    }
}
