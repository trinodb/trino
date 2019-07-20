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
package io.prestosql.plugin.hive.util;

import io.prestosql.plugin.hive.util.LoggingInvocationHandler.AirliftParameterNamesProvider;
import io.prestosql.plugin.hive.util.LoggingInvocationHandler.ReflectiveParameterNamesProvider;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationHandler;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.reflect.Reflection.newProxy;
import static java.lang.reflect.Proxy.newProxyInstance;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLoggingInvocationHandler
{
    private static final String DURATION_PATTERN = "\\d+(\\.\\d+)?\\w{1,2}";

    @Test
    public void testLoggingAndExceptions()
    {
        SomeInterface delegate = new SomeInterface()
        {
            @Override
            public void run(boolean ok, String s)
            {
                if (!ok) {
                    throw new ArrayStoreException(s);
                }
            }
        };
        List<String> messages = new ArrayList<>();
        InvocationHandler handler = new LoggingInvocationHandler(delegate, new ReflectiveParameterNamesProvider(), messages::add);
        SomeInterface proxy = newProxy(SomeInterface.class, handler);

        proxy.run(true, "xyz");

        assertThatThrownBy(() -> proxy.run(false, "bad"))
                .isInstanceOf(ArrayStoreException.class)
                .hasMessage("bad");

        assertThat(messages)
                .hasSize(2)
                .satisfies(list -> {
                    assertThat(list.get(0)).matches("\\QInvocation of run(ok=true, s='xyz') succeeded in\\E " + DURATION_PATTERN);
                    assertThat(list.get(1)).matches("\\QInvocation of run(ok=false, s='bad') took\\E " + DURATION_PATTERN +
                            " \\Qand failed with java.lang.ArrayStoreException: bad\\E");
                });
    }

    @Test
    public void testWithThriftHiveMetastoreClient()
            throws Exception
    {
        List<String> messages = new ArrayList<>();
        // LoggingInvocationHandler is used e.g. with ThriftHiveMetastore.Iface. Since the logging is reflection-based,
        // we test it with this interface as well.
        ThriftHiveMetastore.Iface proxy = newProxy(ThriftHiveMetastore.Iface.class, new LoggingInvocationHandler(
                dummyThriftHiveMetastoreClient(),
                new AirliftParameterNamesProvider(ThriftHiveMetastore.Iface.class, ThriftHiveMetastore.Client.class),
                messages::add));
        proxy.get_table("some_database", "some_table_name");
        assertThat(messages)
                .hasSize(1)
                .element(0).matches(message -> message.matches("\\QInvocation of get_table(dbname='some_database', tbl_name='some_table_name') succeeded in\\E " + DURATION_PATTERN));
    }

    private static ThriftHiveMetastore.Iface dummyThriftHiveMetastoreClient()
    {
        return (ThriftHiveMetastore.Iface) newProxyInstance(
                TestLoggingInvocationHandler.class.getClassLoader(),
                new Class<?>[] {ThriftHiveMetastore.Iface.class},
                (proxy, method, args) -> null);
    }

    private interface SomeInterface
    {
        default void run(boolean ok, String s) {}
    }
}
