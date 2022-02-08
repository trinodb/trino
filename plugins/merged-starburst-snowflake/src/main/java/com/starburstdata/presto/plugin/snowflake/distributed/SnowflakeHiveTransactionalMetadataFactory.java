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

import io.trino.plugin.hive.TransactionalMetadata;
import io.trino.plugin.hive.TransactionalMetadataFactory;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.spi.security.ConnectorIdentity;

import static java.util.Objects.requireNonNull;

public class SnowflakeHiveTransactionalMetadataFactory
        implements TransactionalMetadataFactory
{
    private final SemiTransactionalHiveMetastore metastore;

    public SnowflakeHiveTransactionalMetadataFactory(SemiTransactionalHiveMetastore metastore)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    public TransactionalMetadata create(ConnectorIdentity identity, boolean autoCommit)
    {
        return new TransactionalMetadata()
        {
            @Override
            public SemiTransactionalHiveMetastore getMetastore()
            {
                return metastore;
            }

            @Override
            public void commit()
            {
                if (!metastore.isFinished()) {
                    metastore.commit();
                }
            }

            @Override
            public void rollback()
            {
                metastore.rollback();
            }
        };
    }
}
