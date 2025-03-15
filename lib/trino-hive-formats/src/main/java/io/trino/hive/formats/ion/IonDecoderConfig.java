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
package io.trino.hive.formats.ion;

import java.util.Map;

/**
 * Captures the SerDe properties that affect decoding.
 *
 * @param pathExtractors Map of column name => ion paths
 *        for each entry in the map, the value bound to the column will be the result
 *        of extracting the given search path.
 * @param strictTyping whether the path extractions should enforce type expectations.
 *        this only affects type checking of path extractions; any value decoded into
 *        a trino column will be correctly typed or coerced for that column.
 * @param caseSensitive whether field name matching should be case-sensitive or not.
 */
public record IonDecoderConfig(Map<String, String> pathExtractors, Boolean strictTyping, Boolean caseSensitive)
{
    static IonDecoderConfig defaultConfig()
    {
        return new IonDecoderConfig(Map.of(), false, false);
    }

    IonDecoderConfig withStrictTyping()
    {
        return new IonDecoderConfig(pathExtractors, true, caseSensitive);
    }

    IonDecoderConfig withCaseSensitive()
    {
        return new IonDecoderConfig(pathExtractors, strictTyping, true);
    }

    IonDecoderConfig withPathExtractors(Map<String, String> pathExtractors)
    {
        return new IonDecoderConfig(pathExtractors, strictTyping, caseSensitive);
    }
}
