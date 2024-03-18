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
package io.trino.plugin.varada.dispatcher.warmup;

import io.airlift.log.Logger;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.spi.connector.ConnectorSession;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;
import io.varada.cloudvendors.configuration.StoreType;

public class WarmUtils
{
    public static final int FAST_WARMING_VERSION = 5;

    private static final Logger logger = Logger.get(WarmUtils.class);

    private WarmUtils()
    {
        logger.debug("fast warming version %d", FAST_WARMING_VERSION);
    }

    public static boolean isImportExportEnabled(
            GlobalConfiguration globalConfiguration,
            CloudVendorConfiguration cloudVendorConfiguration,
            ConnectorSession session)
    {
        Boolean sessionEnabled = VaradaSessionProperties.getEnableImportExport(session);
        return sessionEnabled != null ? sessionEnabled :
                globalConfiguration.getEnableImportExport() &&
                        cloudVendorConfiguration.getStoreType() != StoreType.LOCAL;
    }

    public static String getRowGroupStorageObjectName(RowGroupKey rowGroupKey, String path)
    {
        return rowGroupKey.stringFileNameRepresentation(path + "/" + FAST_WARMING_VERSION + "/");
    }

    public static String getCloudPath(RowGroupKey rowGroupKey, String cloudImportExportPath)
    {
        return getRowGroupStorageObjectName(rowGroupKey, cloudImportExportPath);
    }
}
