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
        this.catalogId = catalogId;
    }

    @CanIgnoreReturnValue
    public <B extends AwsRequest.Builder> B configureClient(B baseRequestBuilder)
    {
        catalogId.ifPresent(id -> setCatalogId(baseRequestBuilder, id));
        return baseRequestBuilder;
    }

    private static void setCatalogId(AwsRequest.Builder request, String catalogId)
    {
        if (request instanceof GetDatabasesRequest.Builder) {
            ((GetDatabasesRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof GetDatabaseRequest.Builder) {
            ((GetDatabaseRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof CreateDatabaseRequest.Builder) {
            ((CreateDatabaseRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof UpdateDatabaseRequest.Builder) {
            ((UpdateDatabaseRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof DeleteDatabaseRequest.Builder) {
            ((DeleteDatabaseRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof GetTablesRequest.Builder) {
            ((GetTablesRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof GetTableRequest.Builder) {
            ((GetTableRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof CreateTableRequest.Builder) {
            ((CreateTableRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof UpdateTableRequest.Builder) {
            ((UpdateTableRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof DeleteTableRequest.Builder) {
            ((DeleteTableRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof GetPartitionsRequest.Builder) {
            ((GetPartitionsRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof GetPartitionRequest.Builder) {
            ((GetPartitionRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof UpdatePartitionRequest.Builder) {
            ((UpdatePartitionRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof DeletePartitionRequest.Builder) {
            ((DeletePartitionRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof BatchGetPartitionRequest.Builder) {
            ((BatchGetPartitionRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof BatchCreatePartitionRequest.Builder) {
            ((BatchCreatePartitionRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof BatchUpdatePartitionRequest.Builder) {
            ((BatchUpdatePartitionRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof GetColumnStatisticsForTableRequest.Builder) {
            ((GetColumnStatisticsForTableRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof UpdateColumnStatisticsForTableRequest.Builder) {
            ((UpdateColumnStatisticsForTableRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof DeleteColumnStatisticsForTableRequest.Builder) {
            ((DeleteColumnStatisticsForTableRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof GetColumnStatisticsForPartitionRequest.Builder) {
            ((GetColumnStatisticsForPartitionRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof UpdateColumnStatisticsForPartitionRequest.Builder) {
            ((UpdateColumnStatisticsForPartitionRequest.Builder) request).catalogId(catalogId);
        }
        if (request instanceof DeleteColumnStatisticsForPartitionRequest.Builder) {
            ((DeleteColumnStatisticsForPartitionRequest.Builder) request).catalogId(catalogId);
        }
        throw new IllegalArgumentException("Unsupported request: " + request);
    }
}
