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
package com.starburstdata.presto.plugin.snowflake.distributed;

import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.EncryptionMaterialsProvider;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import javax.crypto.spec.SecretKeySpec;

import java.util.Base64;
import java.util.Map;

import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_ENCRYPTION_MATERIALS_PROVIDER;
import static java.util.Objects.requireNonNull;

public class SnowflakeEncryptionMaterialsProvider
        implements EncryptionMaterialsProvider, Configurable
{
    private static final String QUERY_STAGE_MASTER_KEY = "snowflake.encryption-material";

    private Configuration configuration;

    static void setQueryStageMasterKey(Configuration configuration, String queryStageMasterKey)
    {
        configuration.set(QUERY_STAGE_MASTER_KEY, queryStageMasterKey);
        configuration.set(S3_ENCRYPTION_MATERIALS_PROVIDER, SnowflakeEncryptionMaterialsProvider.class.getName());
    }

    @Override
    public void refresh() {}

    @Override
    public EncryptionMaterials getEncryptionMaterials(Map<String, String> materialsDescription)
    {
        return getEncryptionMaterials();
    }

    @Override
    public EncryptionMaterials getEncryptionMaterials()
    {
        String queryStageMasterKey = requireNonNull(configuration.get(QUERY_STAGE_MASTER_KEY), "queryStageMasterKey is null");
        byte[] decodedKey = Base64.getDecoder().decode(queryStageMasterKey);
        SecretKeySpec queryStageMasterKeySpec = new SecretKeySpec(decodedKey, "AES");
        return new EncryptionMaterials(queryStageMasterKeySpec);
    }

    @Override
    public void setConf(Configuration configuration)
    {
        this.configuration = requireNonNull(configuration, "configuration is null");
    }

    @Override
    public Configuration getConf()
    {
        return configuration;
    }
}
