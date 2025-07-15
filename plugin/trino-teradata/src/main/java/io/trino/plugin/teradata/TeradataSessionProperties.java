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
package io.trino.plugin.teradata;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public final class TeradataSessionProperties
        implements SessionPropertiesProvider
{
    public static final String STRING_PUSHDOWN_ENABLED = "string_pushdown_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public TeradataSessionProperties(TeradataConfig teradataConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        STRING_PUSHDOWN_ENABLED,
                        "Enable string pushdown with collate (experimental)",
                        teradataConfig.isStringPushdownEnabled(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
