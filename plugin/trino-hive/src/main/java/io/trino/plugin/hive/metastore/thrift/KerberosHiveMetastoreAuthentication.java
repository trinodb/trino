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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.trino.plugin.base.authentication.CachingKerberosAuthentication;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.hive.formats.ReadWriteUtils.readVInt;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class KerberosHiveMetastoreAuthentication
        implements HiveMetastoreAuthentication
{
    private final String hiveMetastoreServicePrincipal;
    private final CachingKerberosAuthentication authentication;

    @Inject
    public KerberosHiveMetastoreAuthentication(
            MetastoreKerberosConfig config,
            @ForHiveMetastore CachingKerberosAuthentication authentication)
    {
        this(config.getHiveMetastoreServicePrincipal(), authentication);
    }

    public KerberosHiveMetastoreAuthentication(String hiveMetastoreServicePrincipal, CachingKerberosAuthentication authentication)
    {
        this.hiveMetastoreServicePrincipal = requireNonNull(hiveMetastoreServicePrincipal, "hiveMetastoreServicePrincipal is null");
        this.authentication = requireNonNull(authentication, "authentication is null");
    }

    @Override
    public TTransport authenticate(TTransport rawTransport, String hiveMetastoreHost, Optional<String> delegationToken)
    {
        try {
            Map<String, String> saslProps = ImmutableMap.of(
                    Sasl.QOP, "auth-conf,auth",
                    Sasl.SERVER_AUTH, "true");

            TTransport saslTransport;
            if (delegationToken.isPresent()) {
                saslTransport = new TSaslClientTransport(
                        "DIGEST-MD5", // SaslRpcServer.AuthMethod.TOKEN
                        null,
                        null,
                        "default",
                        saslProps,
                        new SaslClientCallbackHandler(delegationToken.get()),
                        rawTransport);
            }
            else {
                String[] names = hiveMetastoreServicePrincipal.split("[/@]");
                checkArgument(names.length == 3, "Kerberos principal name does not have the expected hostname part: %s", hiveMetastoreServicePrincipal);
                if (names[1].equals("_HOST")) {
                    names[1] = hiveMetastoreHost.toLowerCase(ENGLISH);
                }

                saslTransport = new TSaslClientTransport(
                        "GSSAPI", // SaslRpcServer.AuthMethod.KERBEROS
                        null,
                        names[0],
                        names[1],
                        saslProps,
                        null,
                        rawTransport);
            }

            return new TSubjectAssumingTransport(saslTransport, authentication.getSubject());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (TTransportException e) {
            throw new RuntimeException(e);
        }
    }

    private static class SaslClientCallbackHandler
            implements CallbackHandler
    {
        private final String username;
        private final String password;

        public SaslClientCallbackHandler(String token)
        {
            // see org.apache.hadoop.security.token.Token#decodeFromUrlString
            byte[] decoded = Base64.getUrlDecoder().decode(token);
            SliceInput in = new BasicSliceInput(Slices.wrappedBuffer(decoded));

            byte[] username = new byte[toIntExact(readVInt(in))];
            in.readFully(username);

            byte[] password = new byte[toIntExact(readVInt(in))];
            in.readFully(password);

            this.username = Base64.getEncoder().encodeToString(username);
            this.password = Base64.getEncoder().encodeToString(password);
        }

        @Override
        public void handle(Callback[] callbacks)
        {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback nameCallback) {
                    nameCallback.setName(username);
                }
                if (callback instanceof PasswordCallback passwordCallback) {
                    passwordCallback.setPassword(password.toCharArray());
                }
                if (callback instanceof RealmCallback realmCallback) {
                    realmCallback.setText(realmCallback.getDefaultText());
                }
            }
        }
    }
}
