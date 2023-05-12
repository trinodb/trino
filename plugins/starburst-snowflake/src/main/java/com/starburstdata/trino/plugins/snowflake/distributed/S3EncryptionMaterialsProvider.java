/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.distributed;

import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.EncryptionMaterialsProvider;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import javax.crypto.spec.SecretKeySpec;

import java.util.Base64;
import java.util.Map;

import static com.starburstdata.trino.plugins.snowflake.distributed.HiveUtils.getQueryStageMasterKey;
import static com.starburstdata.trino.plugins.snowflake.distributed.HiveUtils.setQueryStageMasterKey;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_ENCRYPTION_MATERIALS_PROVIDER;
import static java.util.Objects.requireNonNull;

public class S3EncryptionMaterialsProvider
        implements EncryptionMaterialsProvider, Configurable
{
    private Configuration configuration;

    static void configureClientSideEncryption(Configuration configuration, String queryStageMasterKey)
    {
        setQueryStageMasterKey(configuration, queryStageMasterKey);
        configuration.set(S3_ENCRYPTION_MATERIALS_PROVIDER, S3EncryptionMaterialsProvider.class.getName());
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
        String queryStageMasterKey = getQueryStageMasterKey(configuration);
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
