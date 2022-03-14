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
package io.trino.plugin.base.security;

import java.util.function.Supplier;

import static io.trino.plugin.base.security.FileBasedAccessControlUtils.parseJSONString;
import static java.util.Objects.requireNonNull;

public abstract class RulesProvider<R>
        implements Supplier<R>
{
    protected final String jsonPointer;
    protected final Class<R> clazz;

    public RulesProvider(FileBasedAccessControlConfig config, Class<R> clazz) {
        this.clazz = requireNonNull(clazz);
        this.jsonPointer = requireNonNull(config.getJsonPointer());
    }

    protected abstract String getRawJsonString();

    @Override
    public R get()
    {
        return parseJSONString(getRawJsonString(), jsonPointer, clazz);
    }
}
