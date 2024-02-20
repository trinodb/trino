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
package io.trino.plugin.phoenix5;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static java.lang.String.format;

public final class PhoenixSessionProperties
        implements SessionPropertiesProvider
{
    public static final String MAX_SCANS_PER_SPLIT = "max_scans_per_split";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public PhoenixSessionProperties(PhoenixConfig phoenixConfig)
    {
        sessionProperties = ImmutableList.of(
                integerProperty(
                        MAX_SCANS_PER_SPLIT,
                        "Maximum number of HBase scans per split",
                        phoenixConfig.getMaxScansPerSplit(),
                        PhoenixSessionProperties::validateScansPerSplit,
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    private static void validateScansPerSplit(int maxScansPerSplit)
    {
        if (maxScansPerSplit < 1) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s must be greater than 0: %s", MAX_SCANS_PER_SPLIT, maxScansPerSplit));
        }
        if (maxScansPerSplit > PhoenixConfig.MAX_ALLOWED_SCANS_PER_SPLIT) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s cannot exceed %s: %s", MAX_SCANS_PER_SPLIT, PhoenixConfig.MAX_ALLOWED_SCANS_PER_SPLIT, maxScansPerSplit));
        }
    }
}
