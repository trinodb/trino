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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.testing.containers.Httpd;
import io.trino.testing.containers.Minio;

import static io.trino.testing.containers.Httpd.builderForHttpProxy;

public class TestHive2OnDataLakeOverProxy
        extends BaseTestHiveOnDataLake
{
    public TestHive2OnDataLakeOverProxy()
    {
        super(HiveHadoop.DEFAULT_IMAGE);
    }

    @Override
    protected ImmutableMap.Builder<String, String> getAdditionalHivePropertiesBuilder()
    {
        Httpd httpd = closeAfterClass(builderForHttpProxy(8888)
                .addModuleConfiguration("" +
                        "<VirtualHost *:8888>\n" +
                        "    ErrorLog /var/log/http.log\n" +
                        "              \n" +
                        "    ProxyRequests Off\n" +
                        "    ProxyVia Block\n" +
                        "    ProxyPreserveHost On\n" +
                        "             \n" +
                        "    <Proxy *>\n" +
                        "        Require all granted\n" +
                        "    </Proxy>\n" +
                        "             \n" +
                        "    ProxyPass / http://" + Minio.DEFAULT_HOST_NAME + ":" + Minio.MINIO_API_PORT + "/\n" +
                        "    ProxyPassReverse / http://" + Minio.DEFAULT_HOST_NAME + ":" + Minio.MINIO_API_PORT + "/\n" +
                        "</VirtualHost>")
                .withNetwork(network)
                .build());
        httpd.start();
        return super.getAdditionalHivePropertiesBuilder()
                .put("hive.s3.ssl.enabled", "false")
                .put("hive.s3.proxy.protocol", "http")
                .put("hive.s3.proxy.host", "localhost")
                .put("hive.s3.proxy.port", "" + httpd.getListenPort());
    }
}
