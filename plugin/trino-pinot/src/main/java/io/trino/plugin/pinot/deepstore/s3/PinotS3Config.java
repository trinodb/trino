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
package io.trino.plugin.pinot.deepstore.s3;

import io.airlift.configuration.Config;

import java.net.URI;
import java.util.Optional;

public class PinotS3Config
{
    private Optional<String> s3AccessKeyFile = Optional.empty();
    private Optional<String> s3SecretKeyFile = Optional.empty();
    private Optional<URI> s3Endpoint = Optional.empty();
    private Optional<String> s3Region = Optional.empty();

    public Optional<String> getS3AccessKeyFile()
    {
        return s3AccessKeyFile;
    }

    @Config("pinot.s3-accesskey-file")
    public PinotS3Config setS3AccessKeyFile(String s3AccessKeyFile)
    {
        this.s3AccessKeyFile = Optional.ofNullable(s3AccessKeyFile);
        return this;
    }

    public Optional<String> getS3SecretKeyFile()
    {
        return s3SecretKeyFile;
    }

    @Config("pinot.s3-secretkey-file")
    public PinotS3Config setS3SecretKeyFile(String s3SecretKeyFile)
    {
        this.s3SecretKeyFile = Optional.ofNullable(s3SecretKeyFile);
        return this;
    }

    public Optional<URI> getS3Endpoint()
    {
        return s3Endpoint;
    }

    @Config("pinot.s3-endpoint")
    public PinotS3Config setS3Endpoint(String s3Endpoint)
    {
        this.s3Endpoint = Optional.ofNullable(s3Endpoint).map(URI::create);
        return this;
    }

    public Optional<String> getS3Region()
    {
        return s3Region;
    }

    @Config("pinot.s3-region")
    public PinotS3Config setS3Region(String s3Region)
    {
        this.s3Region = Optional.ofNullable(s3Region);
        return this;
    }
}
