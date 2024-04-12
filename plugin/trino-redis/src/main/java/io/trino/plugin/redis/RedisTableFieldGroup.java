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
package io.trino.plugin.redis;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Groups the field descriptions for value or key.
 */
public record RedisTableFieldGroup(
        String dataFormat,
        String name,
        List<RedisTableFieldDescription> fields)
{
    public RedisTableFieldGroup
    {
        requireNonNull(dataFormat, "dataFormat is null");
        if (!dataFormat.equals("set") && !dataFormat.equals("zset")) {
            fields = ImmutableList.copyOf(requireNonNull(fields, "fields is null"));
        }
        else {
            fields = null;
        }
    }
}
