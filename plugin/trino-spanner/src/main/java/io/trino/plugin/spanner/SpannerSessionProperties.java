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
package io.trino.plugin.spanner;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

public class SpannerSessionProperties
        implements SessionPropertiesProvider
{
    public static final String WRITE_MODE = "write_mode";
    private final ImmutableList<PropertyMetadata<?>> sessionProperties;

    public SpannerSessionProperties()
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(PropertyMetadata.enumProperty(WRITE_MODE,
                        "On write mode INSERT spanner throws an error on insert with duplicate primary keys," +
                                "on write mode UPSERT spanner updates the record with existing primary key with the new record being",
                        Mode.class,
                        Mode.INSERT,
                        false)).build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    enum Mode
    {
        INSERT, UPSERT
    }
}
