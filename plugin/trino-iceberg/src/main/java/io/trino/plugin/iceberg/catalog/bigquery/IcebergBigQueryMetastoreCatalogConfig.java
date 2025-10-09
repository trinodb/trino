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
package io.trino.plugin.iceberg.catalog.bigquery;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class IcebergBigQueryMetastoreCatalogConfig
{
    private String projectID;
    private String location;
    private String listAllTables;
    private String warehouse;
    private String jsonKeyFilePath;

    @Config("iceberg.bqms-catalog.project-id")
    @ConfigDescription("The project id to use for BQMS")
    public IcebergBigQueryMetastoreCatalogConfig setProjectID(String projectID)
    {
        this.projectID = projectID;
        return this;
    }

    @Config("iceberg.bqms-catalog.location")
    @ConfigDescription("The location to use for BQMS")
    public IcebergBigQueryMetastoreCatalogConfig setLocation(String location)
    {
        this.location = location;
        return this;
    }

    @Config("iceberg.bqms-catalog.list-all-tables")
    @ConfigDescription("The list all tables config for BQMS")
    public IcebergBigQueryMetastoreCatalogConfig setListAllTables(String listAllTables)
    {
        this.listAllTables = listAllTables;
        return this;
    }

    @Config("iceberg.bqms-catalog.warehouse")
    @ConfigDescription("The list all tables config for BQMS")
    public IcebergBigQueryMetastoreCatalogConfig setWarehouse(String warehouse)
    {
        this.warehouse = warehouse;
        return this;
    }

    @Config("iceberg.bqms-catalog.json-key-file-path")
    @ConfigDescription("Service account will be used to connect BQMS")
    public IcebergBigQueryMetastoreCatalogConfig setJsonKeyFilePath(String jsonKeyFilePath)
    {
        this.jsonKeyFilePath = jsonKeyFilePath;
        return this;
    }

    public String getProjectID()
    {
        return projectID;
    }

    public String getLocation()
    {
        return location;
    }

    public String getListAllTables()
    {
        return listAllTables;
    }

    public String getWarehouse()
    {
        return warehouse;
    }

    public String getJsonKeyFilePath()
    {
        return jsonKeyFilePath;
    }
}
