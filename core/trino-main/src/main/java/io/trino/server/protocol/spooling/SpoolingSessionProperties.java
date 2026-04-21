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
package io.trino.server.protocol.spooling;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.function.Consumer;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;

public class SpoolingSessionProperties
        implements SystemSessionPropertiesProvider
{
    // Spooled segments
    public static final String INITIAL_SEGMENT_SIZE = "spooling_initial_segment_size";
    public static final String MAX_SEGMENT_SIZE = "spooling_max_segment_size";

    // Inlined segments
    public static final String INLINING_ENABLED = "spooling_inlining_enabled";
    public static final String INLINING_MAX_SIZE = "spooling_inlining_max_size";
    public static final String INLINING_MAX_ROWS = "spooling_inlining_max_rows";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public SpoolingSessionProperties(SpoolingConfig spoolingConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(dataSizeProperty(
                        INITIAL_SEGMENT_SIZE,
                        "Initial size of a spooled segment",
                        spoolingConfig.getInitialSegmentSize(),
                        isDataSizeBetween(INITIAL_SEGMENT_SIZE, DataSize.of(1, KILOBYTE), DataSize.of(128, MEGABYTE)),
                        false))
                .add(dataSizeProperty(
                        MAX_SEGMENT_SIZE,
                        "Maximum size of a spooled segment",
                        spoolingConfig.getMaximumSegmentSize(),
                        isDataSizeBetween(MAX_SEGMENT_SIZE, DataSize.of(1, KILOBYTE), DataSize.of(128, MEGABYTE)),
                        false))
                .add(booleanProperty(
                        INLINING_ENABLED,
                        "Allow inlining initial rows",
                        spoolingConfig.isInliningEnabled(),
                        false))
                .add(dataSizeProperty(
                        INLINING_MAX_SIZE,
                        "Maximum size of inlined data",
                        spoolingConfig.getInliningMaxSize(),
                        isDataSizeBetween(INLINING_MAX_SIZE, DataSize.of(1, KILOBYTE), DataSize.of(1, MEGABYTE)),
                        false))
                .add(longProperty(
                        INLINING_MAX_ROWS,
                        "Maximum number of rows that are allowed to be inlined per worker",
                        spoolingConfig.getInliningMaxRows(),
                        false))
                .build();
    }

    private Consumer<DataSize> isDataSizeBetween(String property, DataSize min, DataSize max)
    {
        return value -> {
            if (min.compareTo(value) > 0) {
                throw new TrinoException(INVALID_SESSION_PROPERTY, "Session property '" + property + "' must be greater than " + min);
            }

            if (max.compareTo(value) < 0) {
                throw new TrinoException(INVALID_SESSION_PROPERTY, "Session property '" + property + "' must be smaller than " + max);
            }
        };
    }

    public static DataSize getInitialSegmentSize(Session session)
    {
        return session.getSystemProperty(INITIAL_SEGMENT_SIZE, DataSize.class);
    }

    public static DataSize getMaxSegmentSize(Session session)
    {
        return session.getSystemProperty(MAX_SEGMENT_SIZE, DataSize.class);
    }

    public static boolean isInliningEnabled(Session session)
    {
        return session.getSystemProperty(INLINING_ENABLED, Boolean.class);
    }

    public static DataSize getInliningMaxSize(Session session)
    {
        return session.getSystemProperty(INLINING_MAX_SIZE, DataSize.class);
    }

    public static long getInliningMaxRows(Session session)
    {
        return session.getSystemProperty(INLINING_MAX_ROWS, Long.class);
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
