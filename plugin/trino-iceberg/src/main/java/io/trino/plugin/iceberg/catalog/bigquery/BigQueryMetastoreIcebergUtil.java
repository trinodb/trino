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

import com.google.api.client.util.Maps;
import com.google.api.services.bigquery.model.ExternalCatalogDatasetOptions;
import com.google.api.services.bigquery.model.ExternalCatalogTableOptions;
import com.google.api.services.bigquery.model.SerDeInfo;
import com.google.api.services.bigquery.model.StorageDescriptor;

import java.util.Map;

public class BigQueryMetastoreIcebergUtil
{
    private BigQueryMetastoreIcebergUtil() {}

    private static final String HIVE_SERIALIZATION_LIBRARY =
            "org.apache.iceberg.mr.hive.HiveIcebergSerDe";
    private static final String HIVE_FILE_INPUT_FORMAT =
            "org.apache.iceberg.mr.hive.HiveIcebergInputFormat";
    private static final String HIVE_FILE_OUTPUT_FORMAT =
            "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat";

    /**
     * Creates a new ExternalCatalogTableOptions object populated with the supported library constants
     * and parameters given.
     *
     * @param locationUri storage location uri
     * @param parameters table metadata parameters
     */
    public static ExternalCatalogTableOptions createExternalCatalogTableOptions(
            String locationUri, Map<String, String> parameters)
    {
        SerDeInfo serDeInfo = new SerDeInfo().setSerializationLibrary(HIVE_SERIALIZATION_LIBRARY);

        StorageDescriptor storageDescriptor =
                new StorageDescriptor()
                        .setLocationUri(locationUri)
                        .setInputFormat(HIVE_FILE_INPUT_FORMAT)
                        .setOutputFormat(HIVE_FILE_OUTPUT_FORMAT)
                        .setSerdeInfo(serDeInfo);

        return new ExternalCatalogTableOptions()
                .setStorageDescriptor(storageDescriptor)
                .setParameters(parameters);
    }

    /**
     * Creates a new ExternalCatalogDatasetOptions object populated with the supported library
     * constants and parameters given.
     *
     * @param defaultStorageLocationUri dataset's default location uri
     * @param metadataParameters metadata parameters for the dataset
     */
    public static ExternalCatalogDatasetOptions createExternalCatalogDatasetOptions(
            String defaultStorageLocationUri, Map<String, Object> metadataParameters)
    {
        Map<String, String> elements = Maps.newHashMap();
        metadataParameters.forEach((key, value) -> elements.put(key, value.toString()));
        return new ExternalCatalogDatasetOptions()
                .setDefaultStorageLocationUri(defaultStorageLocationUri)
                .setParameters(elements);
    }
}
