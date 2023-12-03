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
package io.trino.plugin.hive.metastore;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;
import static com.google.common.collect.ImmutableSetMultimap.toImmutableSetMultimap;
import static io.trino.plugin.hive.metastore.MetastoreMethod.DROP_TABLE;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_ALL_VIEWS;
import static io.trino.plugin.hive.metastore.MetastoreMethod.fromMethodName;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestMetastoreMethod
{
    @Test
    void testFromMethodName()
    {
        assertThat(fromMethodName("getAllViews")).isEqualTo(GET_ALL_VIEWS);
        assertThat(fromMethodName("dropTable")).isEqualTo(DROP_TABLE);
    }

    @Test
    void testFromMethodNameInvalid()
    {
        assertThatThrownBy(() -> fromMethodName("doesNotExist"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No enum constant io.trino.plugin.hive.metastore.MetastoreMethod.DOES_NOT_EXIST");
    }

    @Test
    void testEnumNamesMapToMethods()
    {
        Set<String> methodNames = Stream.of(HiveMetastore.class.getMethods())
                .map(Method::getName)
                .collect(toSet());

        for (MetastoreMethod method : MetastoreMethod.values()) {
            assertThat(methodNames).contains(UPPER_UNDERSCORE.to(LOWER_CAMEL, method.name()));
        }
    }

    @Test
    void testMethodNamesUnique()
    {
        Stream.of(HiveMetastore.class.getMethods())
                .collect(toImmutableSetMultimap(Method::getName, identity()))
                .asMap().values().forEach(methods ->
                        assertThat(methods).hasSize(1));
    }
}
