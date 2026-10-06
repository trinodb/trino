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
package io.trino.plugin.jdbc;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import static java.sql.Types.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestJsonWriteMapping
{
    @Test
    void testTypedNullBinding()
            throws Exception
    {
        List<List<Object>> calls = new ArrayList<>();
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {PreparedStatement.class},
                (_, method, arguments) -> {
                    calls.add(List.of(method.getName(), arguments[0], arguments[1]));
                    return null;
                });
        WriteMapping mapping = StandardColumnMappings.jsonWriteMapping(
                "json", SliceWriteFunction.of(VARCHAR, (target, index, value) -> target.setString(index, value.toStringUtf8())));
        mapping.getWriteFunction().setNull(statement, 2);
        assertThat(calls).containsExactly(List.of("setNull", 2, VARCHAR));
    }
}
