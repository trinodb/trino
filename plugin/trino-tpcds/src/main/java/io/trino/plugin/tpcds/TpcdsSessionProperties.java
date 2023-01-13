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
package io.trino.plugin.tpcds;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;

public final class TpcdsSessionProperties
{
    private static final String SPLITS_PER_NODE = "splits_per_node";
    private static final String WITH_NO_SEXISM = "with_no_sexism";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public TpcdsSessionProperties(TpcdsConfig config)
    {
        sessionProperties = ImmutableList.of(
                integerProperty(
                        SPLITS_PER_NODE,
                        "Number of splits created for each worker node",
                        config.getSplitsPerNode(),
                        false),
                booleanProperty(
                        WITH_NO_SEXISM,
                        "With no sexism",
                        config.isWithNoSexism(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static int getSplitsPerNode(ConnectorSession session)
    {
        return session.getProperty(SPLITS_PER_NODE, Integer.class);
    }

    public static boolean isWithNoSexism(ConnectorSession session)
    {
        return session.getProperty(WITH_NO_SEXISM, Boolean.class);
    }
}
