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
package io.trino.plugin.mongodb;

import com.mongodb.ConnectionString;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class TestingMongoAtlasInfoProvider
{
    private TestingMongoAtlasInfoProvider() {}

    public static ConnectionString getConnectionString()
    {
        String atlasConnectionUrl = requireSystemProperty("test.mongodb.atlas.connection-url");
        checkArgument(atlasConnectionUrl.matches("^mongodb(\\+srv)?://.*"));
        return new ConnectionString(atlasConnectionUrl);
    }

    private static String requireSystemProperty(String key)
    {
        return requireNonNull(System.getProperty(key), () -> "system property not set: " + key);
    }
}
