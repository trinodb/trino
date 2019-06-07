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
package io.prestosql.server.security;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;

import static org.ietf.jgss.GSSName.NT_HOSTBASED_SERVICE;
import static org.ietf.jgss.GSSName.NT_USER_NAME;

public enum KerberosNameType
{
    HOSTBASED_SERVICE {
        @Override
        public GSSName getGSSName(GSSManager gssManager, String serviceName, String hostname)
        {
            try {
                return gssManager.createName(serviceName + "@" + hostname, NT_HOSTBASED_SERVICE);
            }
            catch (GSSException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String makeServicePrincipal(String serviceName, String hostname)
        {
            return serviceName + "/" + hostname;
        }
    },
    USER_NAME {
        @Override
        public GSSName getGSSName(GSSManager gssManager, String serviceName, String hostname)
        {
            try {
                return gssManager.createName(serviceName, NT_USER_NAME);
            }
            catch (GSSException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String makeServicePrincipal(String serviceName, String hostname)
        {
            return serviceName;
        }
    };

    public abstract GSSName getGSSName(GSSManager gssManager, String serviceName, String hostname);

    public abstract String makeServicePrincipal(String serviceName, String hostname);
}
