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
package io.trino.plugin.exchange.filesystem;

import io.trino.spi.exchange.ExchangeManagerFactory;
import org.testng.annotations.Test;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFileSystemExchangePlugin
{
    @Test
    public void testCreateWithLocalSchema()
    {
        ExchangeManagerFactory factory = getExchangeManagerFactory();
        factory.create(Map.of("exchange.base-directories", "/tmp/blah"));
        factory.create(Map.of("exchange.base-directories", "file:///tmp/blah"));
        factory.create(Map.of("exchange.base-directories", "file:///tmp/blah,file:///tmp/blah2"));
    }

    @Test
    public void testCreateWithS3Schema()
    {
        ExchangeManagerFactory factory = getExchangeManagerFactory();
        factory.create(Map.of("exchange.base-directories", "s3://some_bucket"));
        factory.create(Map.of(
                "exchange.base-directories", "s3://some_bucket,s3://some_other_bucket",
                "exchange.s3.region", "us-east-1"));
    }

    @Test
    public void testCreateWithGsSchema()
    {
        ExchangeManagerFactory factory = getExchangeManagerFactory();
        factory.create(Map.of("exchange.base-directories", "gs://some_bucket"));
        // gs supports s3 configs
        factory.create(Map.of(
                "exchange.base-directories", "gs://some_bucket,gs://some_other_bucket",
                "exchange.s3.region", "us-east-1"));
    }

    @Test
    public void testCreateWithAbfsSchema()
    {
        ExchangeManagerFactory factory = getExchangeManagerFactory();

        // todo fix test for Azure schema; currently we are getting:
        //
        // 2022-09-05T09:14:13.815-0500 SEVERE Azure Identity => ERROR in EnvironmentCredential: Missing required environment variable AZURE_CLIENT_ID
        //
        // com.google.inject.CreationException: Unable to create injector, see the following errors:
        //
        // 1) [Guice/ErrorInjectingConstructor]: IllegalArgumentException: Invalid URL format. URL: null
        //   at AzureBlobFileSystemExchangeStorage.<init>(AzureBlobFileSystemExchangeStorage.java:98)
        //   while locating AzureBlobFileSystemExchangeStorage
        //   at java.base/NativeMethodAccessorImpl.invoke0(Native Method)
        //   while locating FileSystemExchangeStorage
        assertThatThrownBy(() -> factory.create(Map.of("exchange.base-directories", "abfs://blah@foo-bar.dfs.core.windows.net/foo")))
                .hasMessageContaining("Invalid URL format. URL: null");
    }

    @Test
    public void testCreateWithMixedSchemas()
    {
        ExchangeManagerFactory factory = getExchangeManagerFactory();
        assertThatThrownBy(() -> factory.create(Map.of("exchange.base-directories", "/tmp/blah,s3://some_bucket")))
                .hasMessageContaining("Multiple schemes in exchange base directories");

        // with s3 specific config
        assertThatThrownBy(() ->
                factory.create(Map.of(
                        "exchange.base-directories", "/tmp/blah,s3://some_bucket",
                        "exchange.s3.region", "us-east-1")))
                .hasMessageContaining("Multiple schemes in exchange base directories");
    }

    @Test
    public void testWithUnsupportedSchema()
    {
        ExchangeManagerFactory factory = getExchangeManagerFactory();
        assertThatThrownBy(() -> factory.create(Map.of("exchange.base-directories", "bul:///tmp/blah")))
                .hasMessageContaining("Scheme bul is not supported as exchange spooling storage");

        // with s3 specific config
        assertThatThrownBy(() ->
                factory.create(Map.of(
                        "exchange.base-directories", "bul:///tmp/blah",
                        "exchange.s3.region", "us-east-1")))
                .hasMessageContaining("Scheme bul is not supported as exchange spooling storage");
    }

    @Test
    public void testCreateWithLocalSchemaAndS3Configs()
    {
        ExchangeManagerFactory factory = getExchangeManagerFactory();
        assertThatThrownBy(() ->
                factory.create(Map.of(
                        "exchange.base-directories", "file:///tmp/blah",
                        "exchange.s3.region", "us-east-1")))
                .hasMessageContaining("Configuration property 'exchange.s3.region' was not used");
    }

    private static ExchangeManagerFactory getExchangeManagerFactory()
    {
        return getOnlyElement(new FileSystemExchangePlugin().getExchangeManagerFactories());
    }
}
