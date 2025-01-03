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
package io.trino.sql.dialect.ir;

import com.google.common.collect.ImmutableMap;
import io.trino.sql.newir.Operation.AttributeKey;

import java.util.Map;

public class Dialect
{
    // dialect name
    public static final String IR = "ir";

    public static final String TERMINAL = "terminal";

    private Dialect() {}

    public static void terminalOperation(ImmutableMap.Builder<AttributeKey, Object> builder)
    {
        builder.put(new AttributeKey(IR, TERMINAL), true);
    }

    public static Map<AttributeKey, Object> terminalOperation()
    {
        return ImmutableMap.of(new AttributeKey(IR, TERMINAL), true);
    }
}
