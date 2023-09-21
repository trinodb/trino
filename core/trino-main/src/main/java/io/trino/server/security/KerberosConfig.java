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
package io.trino.server.security;

import io.airlift.configuration.Config;
import io.airlift.configuration.LegacyConfig;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.Optional;

import static io.trino.server.security.KerberosNameType.HOSTBASED_SERVICE;

public class KerberosConfig
{
    public static final String HTTP_SERVER_AUTHENTICATION_KRB5_KEYTAB = "http-server.authentication.krb5.keytab";

    private File kerberosConfig;
    private String serviceName;
    private File keytab;
    private String principalHostname;
    private KerberosNameType nameType = HOSTBASED_SERVICE;
    private Optional<String> userMappingPattern = Optional.empty();
    private Optional<File> userMappingFile = Optional.empty();

    @NotNull
    @FileExists
    public File getKerberosConfig()
    {
        return kerberosConfig;
    }

    // This property name has to match the one from io.airlift.http.client.spnego.KerberosConfig
    @Config("http.authentication.krb5.config")
    public KerberosConfig setKerberosConfig(File kerberosConfig)
    {
        this.kerberosConfig = kerberosConfig;
        return this;
    }

    @NotNull
    public String getServiceName()
    {
        return serviceName;
    }

    @Config("http-server.authentication.krb5.service-name")
    @LegacyConfig("http.server.authentication.krb5.service-name")
    public KerberosConfig setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
        return this;
    }

    @FileExists
    public File getKeytab()
    {
        return keytab;
    }

    @Config("http-server.authentication.krb5.keytab")
    @LegacyConfig("http.server.authentication.krb5.keytab")
    public KerberosConfig setKeytab(File keytab)
    {
        this.keytab = keytab;
        return this;
    }

    public String getPrincipalHostname()
    {
        return principalHostname;
    }

    @Config("http-server.authentication.krb5.principal-hostname")
    @LegacyConfig("http.server.authentication.krb5.principal-hostname")
    public KerberosConfig setPrincipalHostname(String principalHostname)
    {
        this.principalHostname = principalHostname;
        return this;
    }

    @NotNull
    public KerberosNameType getNameType()
    {
        return nameType;
    }

    @Config("http-server.authentication.krb5.name-type")
    @LegacyConfig("http.server.authentication.krb5.name-type")
    public KerberosConfig setNameType(KerberosNameType nameType)
    {
        this.nameType = nameType;
        return this;
    }

    public Optional<String> getUserMappingPattern()
    {
        return userMappingPattern;
    }

    @Config("http-server.authentication.krb5.user-mapping.pattern")
    public KerberosConfig setUserMappingPattern(String userMappingPattern)
    {
        this.userMappingPattern = Optional.ofNullable(userMappingPattern);
        return this;
    }

    public Optional<@FileExists File> getUserMappingFile()
    {
        return userMappingFile;
    }

    @Config("http-server.authentication.krb5.user-mapping.file")
    public KerberosConfig setUserMappingFile(File userMappingFile)
    {
        this.userMappingFile = Optional.ofNullable(userMappingFile);
        return this;
    }
}
