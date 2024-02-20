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
package io.trino.server.testing;

import com.google.common.collect.ImmutableList;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.stringProperty;

public class TestingSystemSessionProperties
        implements SystemSessionPropertiesProvider
{
    public static final String TESTING_SESSION_TIME = "testing_session_time";

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return ImmutableList.<PropertyMetadata<?>>builder()
                .add(stringProperty(
                        TESTING_SESSION_TIME,
                        "Mocked session time",
                        null,
                        false))
                .build();
    }
}
