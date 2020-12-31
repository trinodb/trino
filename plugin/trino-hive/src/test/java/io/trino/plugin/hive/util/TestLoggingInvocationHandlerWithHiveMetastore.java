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

import io.prestosql.plugin.base.util.LoggingInvocationHandler;
import io.prestosql.plugin.base.util.LoggingInvocationHandler.AirliftParameterNamesProvider;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.reflect.Reflection.newProxy;
import static java.lang.reflect.Proxy.newProxyInstance;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLoggingInvocationHandlerWithHiveMetastore
{
    private static final String DURATION_PATTERN = "\\d+(\\.\\d+)?\\w{1,2}";

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
                TestLoggingInvocationHandlerWithHiveMetastore.class.getClassLoader(),
                new Class<?>[] {ThriftHiveMetastore.Iface.class},
                (proxy, method, args) -> null);
    }
}
