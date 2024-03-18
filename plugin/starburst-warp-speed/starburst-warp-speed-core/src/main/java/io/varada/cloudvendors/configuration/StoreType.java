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
package io.varada.cloudvendors.configuration;

import java.util.Arrays;
import java.util.Locale;

/*
    when storetype is azure, need to add to varada.properties:
        - warp-speed.config.azure.connection-string (currently not working with https)
        - hive.azure.abfs-storage-account=
        - hive.azure.abfs-access-key
*/
public enum StoreType
{
    S3("s3"),
    GS("gs"),
    AZURE("azure"),
    LOCAL("local");

    private final String configName;

    StoreType(String configName)
    {
        this.configName = configName;
    }

    public static StoreType ofConfigName(String storagePathParamValue, String storageType)
    {
        if (storageType != null) {
            return Arrays.stream(StoreType.values())
                    .filter(type -> type.configName.equalsIgnoreCase(storageType))
                    .findAny()
                    .orElseThrow(() -> new RuntimeException("Unknown storageTypeParamValue: " + storageType));
        }
        else if (storagePathParamValue == null) {
            return StoreType.LOCAL;
        }
        else {
            return byPathPrefix(storagePathParamValue);
        }
    }

    static StoreType byPathPrefix(String storePath)
    {
        String storePathProtocol = storePath.substring(0, storePath.indexOf(":")).toUpperCase(Locale.US);
        if (storePathProtocol.startsWith("GS")) {
            return StoreType.GS;
        }
        else if (storePathProtocol.startsWith("FILE")) {
            return StoreType.LOCAL;
        }
        else if (storePathProtocol.startsWith("ABFS")) {
            return StoreType.AZURE;
        }
        else if (storePathProtocol.startsWith("S3")) {
            return StoreType.S3;
        }
        else {
            throw new RuntimeException("Unsupported storeType: " + storePathProtocol);
        }
    }

    public String getConfigName()
    {
        return configName;
    }
}
