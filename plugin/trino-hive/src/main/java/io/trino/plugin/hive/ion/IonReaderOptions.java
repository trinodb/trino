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
package io.trino.plugin.hive.ion;

import java.util.Map;

public final class IonReaderOptions
{
    public static final String STRICT_PATH_TYPING_PROPERTY = "ion.path_extractor.strict";
    public static final String STRICT_PATH_TYPING_DEFAULT = "false";

    private IonReaderOptions() {}

    static boolean useStrictPathTyping(Map<String, String> propertiesMap)
    {
        return Boolean.parseBoolean(
                propertiesMap.getOrDefault(STRICT_PATH_TYPING_PROPERTY, STRICT_PATH_TYPING_DEFAULT));
    }
}
