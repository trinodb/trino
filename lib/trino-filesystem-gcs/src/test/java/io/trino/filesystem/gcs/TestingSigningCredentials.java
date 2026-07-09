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
package io.trino.filesystem.gcs;

import com.google.auth.Credentials;
import com.google.auth.ServiceAccountSigner;

import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.List;
import java.util.Map;

public final class TestingSigningCredentials
        extends Credentials
        implements ServiceAccountSigner
{
    private final PrivateKey privateKey;

    public TestingSigningCredentials()
            throws GeneralSecurityException
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        this.privateKey = generator.generateKeyPair().getPrivate();
    }

    @Override
    public String getAuthenticationType()
    {
        return "no-auth";
    }

    @Override
    public Map<String, List<String>> getRequestMetadata(URI uri)
    {
        return Map.of();
    }

    @Override
    public boolean hasRequestMetadata()
    {
        return false;
    }

    @Override
    public boolean hasRequestMetadataOnly()
    {
        return true;
    }

    @Override
    public void refresh() {}

    @Override
    public String getAccount()
    {
        return "test-signer@test-project.iam.gserviceaccount.com";
    }

    @Override
    public byte[] sign(byte[] toSign)
    {
        try {
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initSign(privateKey);
            signature.update(toSign);
            return signature.sign();
        }
        catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }
}
