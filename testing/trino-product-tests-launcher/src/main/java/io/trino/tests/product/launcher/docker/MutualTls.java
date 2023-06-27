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
package io.trino.tests.product.launcher.docker;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;

import javax.security.auth.x500.X500Principal;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.List;

import static java.nio.file.Files.createDirectories;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class MutualTls
{
    private static final Logger log = Logger.get(MutualTls.class);
    private static final Provider BOUNCY_PROVIDER = new BouncyCastleProvider();
    private static final String JKS_DEFAULT_PASSWORD = "123456";

    private final ImmutableList.Builder<ServerCertificatePair> builder = ImmutableList.builder();

    public MutualTls addServerCert(String hostName, List<String> altNames)
    {
        X500Principal subject = new X500Principal("CN=" + hostName + ", O=Trino, OU=Engineering");
        KeyPair keyPair = generateKeyPair();

        long notBefore = System.currentTimeMillis();
        long notAfter = notBefore + (1000L * 3600L * 24 * 365); // year

        KeyPurposeId[] purposes = new KeyPurposeId[] {KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_clientAuth};
        X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(subject, BigInteger.ONE, new Date(notBefore), new Date(notAfter), subject, keyPair.getPublic());

        try {
            certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
            certBuilder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature + KeyUsage.keyEncipherment));
            certBuilder.addExtension(Extension.extendedKeyUsage, false, new ExtendedKeyUsage(purposes));
            certBuilder.addExtension(Extension.subjectAlternativeName, false, new DERSequence(getAltNames(hostName, altNames)));

            final ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA")
                    .setProvider(BOUNCY_PROVIDER)
                    .build(keyPair.getPrivate());

            X509CertificateHolder certHolder = certBuilder.build(signer);
            X509Certificate cert = new JcaX509CertificateConverter()
                    .setProvider(BOUNCY_PROVIDER)
                    .getCertificate(certHolder);
            builder.add(new ServerCertificatePair(hostName, keyPair, cert));

            log.info("Generated self-signed certificate for %s (altNames = %s)", hostName, altNames);
            return this;
        }
        catch (OperatorCreationException | IOException | CertificateException e) {
            throw new RuntimeException(e);
        }
    }

    public JKS writeJKS()
            throws IOException
    {
        File keyStorePath = Files.createTempFile("ptl-tls", ".jks").toFile();

        try {
            KeyStore keyStore = KeyStore.getInstance("PKCS12", BOUNCY_PROVIDER);
            if (keyStorePath.exists()) {
                try {
                    keyStore.load(new FileInputStream(keyStorePath), JKS_DEFAULT_PASSWORD.toCharArray());
                }
                catch (EOFException ignored) {
                    // empty file - initialize new keystore
                    keyStore.load(null, null);
                }
            }
            else {
                keyStore.load(null, null);
            }

            List<ServerCertificatePair> pairs = builder.build();
            for (ServerCertificatePair pair : pairs) {
                keyStore.setKeyEntry(pair.hostName(), pair.keyPair().getPrivate(), JKS_DEFAULT_PASSWORD.toCharArray(), new Certificate[] {pair.certificate()});
            }

            log.info("Exporting certificates for: %s to JKS file: %s", pairs.stream().map(ServerCertificatePair::hostName).collect(joining(", ")), keyStorePath);
            keyStore.store(new FileOutputStream(keyStorePath), JKS_DEFAULT_PASSWORD.toCharArray());

            return new JKS(keyStorePath, JKS_DEFAULT_PASSWORD);
        }
        catch (KeyStoreException | CertificateException | IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static KeyPair generateKeyPair()
    {
        try {
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA", BOUNCY_PROVIDER);
            keyPairGenerator.initialize(2048, new SecureRandom());
            return keyPairGenerator.generateKeyPair();
        }
        catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    private static ASN1Encodable[] getAltNames(String hostName, List<String> altNames)
    {
        ImmutableList.Builder<ASN1Encodable> names = ImmutableList.builder();
        names.add(new GeneralName(GeneralName.dNSName, hostName));
        altNames.forEach(name -> names.add(new GeneralName(GeneralName.dNSName, name)));
        return names.build().toArray(new ASN1Encodable[0]);
    }

    public record JKS(File path, String password)
    {
        public JKS
        {
            requireNonNull(path, "path is null");
            requireNonNull(password, "password is null");
        }
    }

    public record ServerCertificatePair(String hostName, KeyPair keyPair, Certificate certificate)
    {
        public ServerCertificatePair
        {
            requireNonNull(hostName, "hostName is null");
            requireNonNull(keyPair, "keyPair is null");
            requireNonNull(certificate, "certificate is null");

            try {
                certificate.verify(keyPair.getPublic(), BOUNCY_PROVIDER);
            }
            catch (CertificateException | NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
                throw new RuntimeException(e);
            }
        }

        public void writeTo(Path basePath)
                throws IOException
        {
            Path outputPath = basePath.resolve(hostName);
            createDirectories(outputPath);
            writePem("PUBLIC KEY", keyPair.getPublic().getEncoded(), outputPath.resolve("public.key").toFile());
            writePem("PRIVATE KEY", keyPair.getPublic().getEncoded(), outputPath.resolve("private.key").toFile());
            writePem("CERTIFICATE", keyPair.getPublic().getEncoded(), outputPath.resolve("certificate.crt").toFile());
        }

        private static void writePem(String type, byte[] encodedContent, File path)
                throws IOException
        {
            try (PemWriter writer = new PemWriter(new FileWriter(path))) {
                writer.writeObject(new PemObject(type, encodedContent));
                writer.flush();
            }
        }
    }
}
