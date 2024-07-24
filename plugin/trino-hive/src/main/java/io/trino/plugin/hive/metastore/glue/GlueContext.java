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
package io.trino.plugin.hive.metastore.glue;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.inject.Inject;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.DeleteColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.UpdateColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class GlueContext
{
    private final Optional<String> catalogId;

    @Inject
    public GlueContext(GlueHiveMetastoreConfig config)
    {
        this(config.getCatalogId());
    }

    public GlueContext(Optional<String> catalogId)
    {
        this.catalogId = requireNonNull(catalogId, "catalogId is null");
    }

    @CanIgnoreReturnValue
    public <B extends AwsRequest.Builder> B configureClient(B baseRequestBuilder)
    {
        catalogId.ifPresent(id -> setCatalogId(baseRequestBuilder, id));
        return baseRequestBuilder;
    }

    private static void setCatalogId(AwsRequest.Builder request, String catalogId)
    {
        switch (request) {
            case GetDatabasesRequest.Builder builder -> builder.catalogId(catalogId);
            case GetDatabaseRequest.Builder builder -> builder.catalogId(catalogId);
            case CreateDatabaseRequest.Builder builder -> builder.catalogId(catalogId);
            case UpdateDatabaseRequest.Builder builder -> builder.catalogId(catalogId);
            case DeleteDatabaseRequest.Builder builder -> builder.catalogId(catalogId);
            case GetTablesRequest.Builder builder -> builder.catalogId(catalogId);
            case GetTableRequest.Builder builder -> builder.catalogId(catalogId);
            case CreateTableRequest.Builder builder -> builder.catalogId(catalogId);
            case UpdateTableRequest.Builder builder -> builder.catalogId(catalogId);
            case DeleteTableRequest.Builder builder -> builder.catalogId(catalogId);
            case GetPartitionsRequest.Builder builder -> builder.catalogId(catalogId);
            case GetPartitionRequest.Builder builder -> builder.catalogId(catalogId);
            case UpdatePartitionRequest.Builder builder -> builder.catalogId(catalogId);
            case DeletePartitionRequest.Builder builder -> builder.catalogId(catalogId);
            case BatchGetPartitionRequest.Builder builder -> builder.catalogId(catalogId);
            case BatchCreatePartitionRequest.Builder builder -> builder.catalogId(catalogId);
            case BatchUpdatePartitionRequest.Builder builder -> builder.catalogId(catalogId);
            case GetColumnStatisticsForTableRequest.Builder builder -> builder.catalogId(catalogId);
            case UpdateColumnStatisticsForTableRequest.Builder builder -> builder.catalogId(catalogId);
            case DeleteColumnStatisticsForTableRequest.Builder builder -> builder.catalogId(catalogId);
            case GetColumnStatisticsForPartitionRequest.Builder builder -> builder.catalogId(catalogId);
            case UpdateColumnStatisticsForPartitionRequest.Builder builder -> builder.catalogId(catalogId);
            case DeleteColumnStatisticsForPartitionRequest.Builder builder -> builder.catalogId(catalogId);
            default -> throw new IllegalArgumentException("Unsupported request: " + request);
        }
    }
}
