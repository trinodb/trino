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
package io.trino.tests.product.hive;

import java.util.HashMap;
import java.util.Map;

public class HiveKerberosHdfsImpersonationWithWireEncryptionEnvironment
        extends HiveKerberosHdfsImpersonationEnvironment
{
    @Override
    protected Map<String, String> getCoreSiteProperties()
    {
        return Map.of("hadoop.rpc.protection", "privacy");
    }

    @Override
    protected Map<String, String> getHdfsSiteProperties()
    {
        return Map.of("dfs.encrypt.data.transfer", "true");
    }

    @Override
    protected Map<String, String> getHdfsClientSiteProperties()
    {
        return Map.of(
                "hadoop.rpc.protection", "privacy",
                "dfs.encrypt.data.transfer", "true");
    }

    @Override
    protected Map<String, String> getAdditionalCatalogProperties()
    {
        Map<String, String> properties = new HashMap<>(super.getAdditionalCatalogProperties());
        properties.put("hive.hdfs.wire-encryption.enabled", "true");
        return Map.copyOf(properties);
    }
}
