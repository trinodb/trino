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
package io.varada.tools.util;

import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PathUtils
{
    private PathUtils()
    {
    }

    // Since for "s3://xxx/model_entities" Path.of() results in "s3:/xxxx/model_entities"
    public static String getUriPath(String firstPart, String... parts)
    {
        String partsStr = Stream.of(parts)
                .filter(StringUtils::isNotEmpty)
                .map(part -> part.startsWith("/") ? part.substring(1) : part)
                .map(part -> part.endsWith("/") ? part.substring(0, part.length() - 1) : part)
                .collect(Collectors.joining("/"));
        return firstPart.endsWith("/") ? firstPart + partsStr : firstPart + "/" + partsStr;
    }

    public static String getLocalPath(String firstPart, String... parts)
    {
        return Path.of(firstPart, parts).toString();
    }
}
