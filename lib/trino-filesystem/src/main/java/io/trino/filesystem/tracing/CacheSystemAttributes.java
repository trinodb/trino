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
package io.trino.filesystem.tracing;

import io.opentelemetry.api.common.AttributeKey;

import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;

public class CacheSystemAttributes
{
    private CacheSystemAttributes() {}

    public static final AttributeKey<String> CACHE_KEY = stringKey("trino.cache.key");
    public static final AttributeKey<String> CACHE_FILE_LOCATION = stringKey("trino.cache.file.location");
    public static final AttributeKey<Long> CACHE_FILE_READ_SIZE = longKey("trino.cache.read_size");
    public static final AttributeKey<Long> CACHE_FILE_READ_POSITION = longKey("trino.cache.read_position");
    public static final AttributeKey<Long> CACHE_FILE_WRITE_SIZE = longKey("trino.cache.write_size");
    public static final AttributeKey<Long> CACHE_FILE_WRITE_POSITION = longKey("trino.cache.write_position");
}
