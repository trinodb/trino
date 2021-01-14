/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.distributed;

import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.EncryptionMaterialsProvider;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import javax.crypto.spec.SecretKeySpec;

import java.util.Base64;
import java.util.Map;

import static io.trino.plugin.hive.s3.PrestoS3FileSystem.S3_ENCRYPTION_MATERIALS_PROVIDER;
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
