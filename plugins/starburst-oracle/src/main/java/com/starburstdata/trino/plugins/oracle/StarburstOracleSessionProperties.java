/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.oracle;

import com.google.common.collect.ImmutableList;
import com.starburstdata.presto.license.LicenseManager;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;

import static com.starburstdata.trino.plugins.oracle.OracleParallelismType.NO_PARALLELISM;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;

public final class StarburstOracleSessionProperties
        implements SessionPropertiesProvider
{
    public static final String PARALLELISM_TYPE = "parallelism_type";
    public static final String MAX_SPLITS_PER_SCAN = "max_splits_per_scan";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public StarburstOracleSessionProperties(LicenseManager licenseManager, StarburstOracleConfig starburstOracleConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(enumProperty(
                        PARALLELISM_TYPE,
                        "Parallelism strategy for reads",
                        OracleParallelismType.class,
                        starburstOracleConfig.getParallelismType(),
                        value -> {
                            if (value != NO_PARALLELISM) {
                                licenseManager.checkLicense();
                            }
                        },
                        false))
                .add(integerProperty(
                        MAX_SPLITS_PER_SCAN,
                        "Maximum number of splits for a table scan",
                        starburstOracleConfig.getMaxSplitsPerScan(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static OracleParallelismType getParallelismType(ConnectorSession session)
    {
        return session.getProperty(PARALLELISM_TYPE, OracleParallelismType.class);
    }

    public static int getMaxSplitsPerScan(ConnectorSession session)
    {
        return session.getProperty(MAX_SPLITS_PER_SCAN, Integer.class);
    }
}
