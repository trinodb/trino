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
package io.prestosql.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.ForHiveMetastore;
import io.prestosql.plugin.hive.authentication.HadoopAuthentication;
import io.prestosql.plugin.hive.authentication.HiveMetastoreAuthentication;
import io.prestosql.plugin.hive.authentication.MetastoreKerberosConfig;
import org.apache.hadoop.hive.metastore.security.DelegationTokenIdentifier;
import org.apache.hadoop.hive.thrift.client.TUGIAssumingTransport;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.token.Token;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TTransport;

import javax.inject.Inject;
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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.KERBEROS;
import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.TOKEN;
import static org.apache.hadoop.security.SecurityUtil.getServerPrincipal;

public class KerberosHiveMetastoreAuthentication
        implements HiveMetastoreAuthentication
{
    private final String hiveMetastoreServicePrincipal;
    private final HadoopAuthentication authentication;

    @Inject
    public KerberosHiveMetastoreAuthentication(
            MetastoreKerberosConfig config,
            @ForHiveMetastore HadoopAuthentication authentication)
    {
        this(config.getHiveMetastoreServicePrincipal(), authentication);
    }

    public KerberosHiveMetastoreAuthentication(String hiveMetastoreServicePrincipal, HadoopAuthentication authentication)
    {
        this.hiveMetastoreServicePrincipal = requireNonNull(hiveMetastoreServicePrincipal, "hiveMetastoreServicePrincipal is null");
        this.authentication = requireNonNull(authentication, "authentication is null");
    }

    @Override
    public TTransport authenticate(TTransport rawTransport, String hiveMetastoreHost, Optional<String> delegationToken)
    {
        try {
            String serverPrincipal = getServerPrincipal(hiveMetastoreServicePrincipal, hiveMetastoreHost);
            String[] names = SaslRpcServer.splitKerberosName(serverPrincipal);
            checkState(names.length == 3,
                    "Kerberos principal name does NOT have the expected hostname part: %s", serverPrincipal);

            Map<String, String> saslProps = ImmutableMap.of(
                    Sasl.QOP, "auth-conf,auth",
                    Sasl.SERVER_AUTH, "true");

            TTransport saslTransport;
            if (delegationToken.isPresent()) {
                saslTransport = new TSaslClientTransport(
                        TOKEN.getMechanismName(),
                        null,
                        null,
                        "default",
                        saslProps,
                        new SaslClientCallbackHandler(decodeDelegationToken(delegationToken.get())),
                        rawTransport);
            }
            else {
                saslTransport = new TSaslClientTransport(
                        KERBEROS.getMechanismName(),
                        null,
                        names[0],
                        names[1],
                        saslProps,
                        null,
                        rawTransport);
            }

            return new TUGIAssumingTransport(saslTransport, authentication.getUserGroupInformation());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Token<DelegationTokenIdentifier> decodeDelegationToken(String tokenValue)
            throws IOException
    {
        Token<DelegationTokenIdentifier> token = new Token<>();
        token.decodeFromUrlString(tokenValue);
        return token;
    }

    private static class SaslClientCallbackHandler
            implements CallbackHandler
    {
        private final String username;
        private final String password;

        SaslClientCallbackHandler(Token<DelegationTokenIdentifier> token)
        {
            this.username = Base64.getEncoder().encodeToString(token.getIdentifier());
            this.password = Base64.getEncoder().encodeToString(token.getPassword());
        }

        @Override
        public void handle(Callback[] callbacks)
        {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    ((NameCallback) callback).setName(username);
                }
                if (callback instanceof PasswordCallback) {
                    ((PasswordCallback) callback).setPassword(password.toCharArray());
                }
                if (callback instanceof RealmCallback) {
                    RealmCallback realmCallback = (RealmCallback) callback;
                    realmCallback.setText(realmCallback.getDefaultText());
                }
            }
        }
    }
}
