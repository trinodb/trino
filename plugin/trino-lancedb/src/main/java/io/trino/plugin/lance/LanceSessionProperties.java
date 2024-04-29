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
package io.trino.plugin.lance;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;


public class LanceSessionProperties
{
    private static final String CONNECTION_TIMEOUT = "connection_timeout";
    // TODO: should we named as enum, HTTP vs JNI? or HTTP vs FRAGMENT.
    private static final String IS_JNI = "is_jni";
    private static final String RETRY_COUNT = "retry_count";

    private final List<PropertyMetadata<?>> sessionProperties;

    public static Duration getConnectionTimeout(ConnectorSession session)
    {
        return session.getProperty(CONNECTION_TIMEOUT, Duration.class);
    }

    public static int getRetryCount(ConnectorSession session)
    {
        return session.getProperty(RETRY_COUNT, Integer.class);
    }

    @Inject
    public LanceSessionProperties(LanceConfig lanceConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        IS_JNI,
                        "Prefer queries to broker even when parallel scan is enabled for aggregation queries",
                        lanceConfig.isJni(),
                        false),
                integerProperty(
                        RETRY_COUNT,
                        "Retry count for retriable pinot data fetch calls",
                        lanceConfig.getFetchRetryCount(),
                        false),
                durationProperty(
                        CONNECTION_TIMEOUT,
                        "Connection Timeout to talk to Pinot servers",
                        lanceConfig.getConnectionTimeout(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
