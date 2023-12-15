/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamicfiltering;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static java.lang.String.format;

public class DynamicRowFilteringSessionProperties
        implements SessionPropertiesProvider
{
    public static final String DYNAMIC_ROW_FILTERING_ENABLED = "dynamic_row_filtering_enabled";
    public static final String DYNAMIC_ROW_FILTERING_SELECTIVITY_THRESHOLD = "dynamic_row_filtering_selectivity_threshold";
    public static final String DYNAMIC_ROW_FILTERING_WAIT_TIMEOUT = "dynamic_row_filtering_wait_timeout";
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public DynamicRowFilteringSessionProperties(@ForDynamicRowFiltering Runnable featureLicenseCheck, DynamicRowFilteringConfig config)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        DYNAMIC_ROW_FILTERING_ENABLED,
                        "Enable dynamic row filtering on worker nodes",
                        config.isDynamicRowFilteringEnabled(),
                        value -> {
                            if (value) {
                                featureLicenseCheck.run();
                            }
                        },
                        false),
                doubleProperty(
                        DYNAMIC_ROW_FILTERING_SELECTIVITY_THRESHOLD,
                        "Avoid using dynamic row filters when fraction of rows selected is above threshold",
                        config.getDynamicRowFilterSelectivityThreshold(),
                        value -> {
                            if (value < 0 || value > 1) {
                                throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s must be in the range [0, 1]: %s", DYNAMIC_ROW_FILTERING_SELECTIVITY_THRESHOLD, value));
                            }
                        },
                        false),
                durationProperty(
                        DYNAMIC_ROW_FILTERING_WAIT_TIMEOUT,
                        "Duration to wait for completion of dynamic filters",
                        config.getDynamicRowFilteringWaitTimeout(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isDynamicRowFilteringEnabled(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_ROW_FILTERING_ENABLED, Boolean.class);
    }

    public static double getDynamicRowFilterSelectivityThreshold(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_ROW_FILTERING_SELECTIVITY_THRESHOLD, Double.class);
    }

    public static Duration getDynamicRowFilteringWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_ROW_FILTERING_WAIT_TIMEOUT, Duration.class);
    }
}
