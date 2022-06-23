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
package io.trino.plugin.hive.metastore.dlf;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.metastore.Column;
import io.trino.spi.TrinoException;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.metastore.dlf.utils.ExpressionUtils.buildDlfExpression;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestAlibabaDlfExpressionUtil
{
    @Test
    public void testBuildDlfExpressionNullValue()
    {
        String foo = buildDlfExpression(new ArrayList<>(), new ArrayList<>());
        assertNull(foo);
    }

    @Test
    public void testBuildDlfExpressionNullKey()
    {
        List<String> value = new ArrayList<>();
        value.add("value");
        assertThatThrownBy(() -> buildDlfExpression(new ArrayList<>(), value)).isInstanceOf(TrinoException.class);
    }

    @Test
    public void testBuildDlfExpressionNotEqualKeyValue()
    {
        List<Column> key = ImmutableList.<Column>builder()
                .add(column("name", HIVE_STRING))
                .build();
        List<String> value = new ArrayList<>();
        value.add("Alice");
        value.add("Bob");
        assertThatThrownBy(() -> buildDlfExpression(key, value)).isInstanceOf(TrinoException.class);
    }

    @Test
    public void testBuildDlfExpressionString()
    {
        List<Column> key = ImmutableList.<Column>builder()
                .add(column("name", HIVE_STRING))
                .build();
        List<String> value = new ArrayList<>();
        value.add("Alice");
        String foo = buildDlfExpression(key, value);
        assertEquals(foo, "(name='Alice')");
    }

    @Test
    public void testBuildDlfExpressionInt()
    {
        List<Column> key = ImmutableList.<Column>builder()
                .add(column("id", HIVE_INT))
                .build();
        List<String> value = new ArrayList<>();
        value.add("1");
        String foo = buildDlfExpression(key, value);
        assertEquals(foo, "(id=1)");
    }

    static Column column(String name, HiveType type)
    {
        return new Column(name, type, Optional.of(""));
    }
}
