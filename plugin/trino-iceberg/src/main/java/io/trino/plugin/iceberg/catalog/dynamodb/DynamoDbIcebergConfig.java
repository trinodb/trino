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
package io.trino.plugin.iceberg.catalog.dynamodb;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import java.util.Optional;

import static org.apache.iceberg.aws.AwsProperties.DYNAMODB_TABLE_NAME_DEFAULT;

public class DynamoDbIcebergConfig
{
    private String catalogName;
    private String tableName = DYNAMODB_TABLE_NAME_DEFAULT;
    private String defaultWarehouseDir;

    private String connectionUrl;
    private String accessKey;
    private String secretKey;
    private String region;
    private String iamRole;
    private String externalId;

    @NotNull
    public String getCatalogName()
    {
        return catalogName;
    }

    @Config("iceberg.dynamodb.catalog-name")
    public DynamoDbIcebergConfig setCatalogName(String catalogName)
    {
        this.catalogName = catalogName;
        return this;
    }

    public String getTableName()
    {
        return tableName;
    }

    @Config("iceberg.dynamodb.table-name")
    @ConfigDescription("Iceberg DynamoDB catalog table name")
    public DynamoDbIcebergConfig setTableName(String tableName)
    {
        this.tableName = tableName;
        return this;
    }

    @NotEmpty
    public String getDefaultWarehouseDir()
    {
        return defaultWarehouseDir;
    }

    @Config("iceberg.dynamodb.default-warehouse-dir")
    @ConfigDescription("The default warehouse directory to use for DynamoDB")
    public DynamoDbIcebergConfig setDefaultWarehouseDir(String defaultWarehouseDir)
    {
        this.defaultWarehouseDir = defaultWarehouseDir;
        return this;
    }

    @NotNull
    public Optional<String> getConnectionUrl()
    {
        return Optional.ofNullable(connectionUrl);
    }

    @Config("iceberg.dynamodb.connection-url")
    @ConfigDescription("Optional URI to connect to the DynamoDB server")
    public DynamoDbIcebergConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    @NotNull
    public Optional<String> getAccessKey()
    {
        return Optional.ofNullable(accessKey);
    }

    @Config("iceberg.dynamodb.aws.access-key")
    @ConfigDescription("User name for DynamoDB client")
    public DynamoDbIcebergConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    @NotNull
    public Optional<String> getSecretKey()
    {
        return Optional.ofNullable(secretKey);
    }

    @Config("iceberg.dynamodb.aws.secret-key")
    @ConfigSecuritySensitive
    @ConfigDescription("Password for DynamoDB client")
    public DynamoDbIcebergConfig setSecretKey(String secretKey)
    {
        this.secretKey = secretKey;
        return this;
    }

    public String getRegion()
    {
        return region;
    }

    @Config("iceberg.dynamodb.aws.region")
    public DynamoDbIcebergConfig setRegion(String region)
    {
        this.region = region;
        return this;
    }

    @NotNull
    public Optional<String> getIamRole()
    {
        return Optional.ofNullable(iamRole);
    }

    @Config("iceberg.dynamodb.aws.iam-role")
    @ConfigDescription("Optional AWS IAM role to assume for authenticating. If set, this role will be used to get credentials to sign requests to ES.")
    public DynamoDbIcebergConfig setIamRole(String iamRole)
    {
        this.iamRole = iamRole;
        return this;
    }

    @NotNull
    public Optional<String> getExternalId()
    {
        return Optional.ofNullable(externalId);
    }

    @Config("iceberg.dynamodb.external-id")
    @ConfigDescription("Optional external id to pass to AWS STS while assuming a role")
    public DynamoDbIcebergConfig setExternalId(String externalId)
    {
        this.externalId = externalId;
        return this;
    }
}
