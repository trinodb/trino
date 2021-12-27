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
package io.trino.client;

import com.google.common.collect.ImmutableSet;

import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.DSAKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.io.Files.asCharSource;
import static io.trino.client.DerUtils.decodeSequence;
import static io.trino.client.DerUtils.decodeSequenceOptionalElement;
import static io.trino.client.DerUtils.encodeOctetString;
import static io.trino.client.DerUtils.encodeOid;
import static io.trino.client.DerUtils.encodeSequence;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Base64.getMimeDecoder;
import static java.util.Locale.US;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static javax.crypto.Cipher.DECRYPT_MODE;

// copy of https://github.com/airlift/airlift/blob/master/security/src/main/java/io/airlift/security/pem/PemReader.java
final class PemReader
{
    private static final Pattern CERT_PATTERN = Pattern.compile(
            "-+BEGIN\\s+.*CERTIFICATE[^-]*-+(?:\\s|\\r|\\n)+" + // Header
                    "([a-z0-9+/=\\r\\n]+)" +                    // Base64 text
                    "-+END\\s+.*CERTIFICATE[^-]*-+",            // Footer
            CASE_INSENSITIVE);

    static final Pattern PRIVATE_KEY_PATTERN = Pattern.compile(
            "-+BEGIN\\s+(?:(.*)\\s+)?PRIVATE\\s+KEY[^-]*-+(?:\\s|\\r|\\n)+" + // Header
                    "([a-z0-9+/=\\r\\n]+)" +                                  // Base64 text
                    "-+END\\s+.*PRIVATE\\s+KEY[^-]*-+",                       // Footer
            CASE_INSENSITIVE);

    static final Pattern PUBLIC_KEY_PATTERN = Pattern.compile(
            "-+BEGIN\\s+(?:(.*)\\s+)?PUBLIC\\s+KEY[^-]*-+(?:\\s|\\r|\\n)+" + // Header
                    "([a-z0-9+/=\\r\\n]+)" +                      // Base64 text
                    "-+END\\s+.*PUBLIC\\s+KEY[^-]*-+",            // Footer
            CASE_INSENSITIVE);

    // test data must be exactly 20 bytes for DSA
    private static final byte[] TEST_SIGNATURE_DATA = "01234567890123456789".getBytes(US_ASCII);
    private static final Set<String> SUPPORTED_KEY_TYPES = ImmutableSet.of("RSA", "EC", "DSA");

    private static final byte[] VERSION_0_ENCODED = {2, 1, 0};
    private static final byte[] RSA_KEY_OID = encodeOid("1.2.840.113549.1.1.1");
    private static final byte[] DSA_KEY_OID = encodeOid("1.2.840.10040.4.1");
    private static final byte[] EC_KEY_OID = encodeOid("1.2.840.10045.2.1");
    private static final byte[] DER_NULL = {5, 0};

    private PemReader() {}

    public static boolean isPem(File file)
            throws IOException
    {
        return isPem(asCharSource(file, US_ASCII).read());
    }

    public static boolean isPem(String data)
    {
        return isPemCertificate(data) ||
                isPemPublicKey(data) ||
                isPemPrivateKey(data);
    }

    private static boolean isPemPrivateKey(String data)
    {
        return PRIVATE_KEY_PATTERN.matcher(data).find();
    }

    private static boolean isPemPublicKey(String data)
    {
        return PUBLIC_KEY_PATTERN.matcher(data).find();
    }

    private static boolean isPemCertificate(String data)
    {
        return CERT_PATTERN.matcher(data).find();
    }

    public static KeyStore loadKeyStore(File certificateChainFile, File privateKeyFile, Optional<String> keyPassword)
            throws IOException, GeneralSecurityException
    {
        PrivateKey key = loadPrivateKey(privateKeyFile, keyPassword);

        List<X509Certificate> certificateChain = readCertificateChain(certificateChainFile);
        if (certificateChain.isEmpty()) {
            throw new CertificateException("Certificate file does not contain any certificates: " + certificateChainFile);
        }

        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);

        // ensure there is a certificate that matches the private key
        Certificate[] certificates = certificateChain.toArray(new Certificate[0]);
        boolean foundMatchingCertificate = false;
        for (int i = 0; i < certificates.length; i++) {
            Certificate certificate = certificates[i];
            if (matches(key, certificate)) {
                foundMatchingCertificate = true;
                // certificate for private key must be in index zero
                certificates[i] = certificates[0];
                certificates[0] = certificate;
                break;
            }
        }
        if (!foundMatchingCertificate) {
            throw new KeyStoreException("Private key does not match the public key of any certificate");
        }

        keyStore.setKeyEntry("key", key, new char[0], certificates);
        return keyStore;
    }

    public static List<X509Certificate> readCertificateChain(File certificateChainFile)
            throws IOException, GeneralSecurityException
    {
        String contents = asCharSource(certificateChainFile, US_ASCII).read();
        return readCertificateChain(contents);
    }

    public static List<X509Certificate> readCertificateChain(String certificateChain)
            throws CertificateException
    {
        Matcher matcher = CERT_PATTERN.matcher(certificateChain);
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        List<X509Certificate> certificates = new ArrayList<>();

        int start = 0;
        while (matcher.find(start)) {
            byte[] buffer = base64Decode(matcher.group(1));
            certificates.add((X509Certificate) certificateFactory.generateCertificate(new ByteArrayInputStream(buffer)));
            start = matcher.end();
        }

        return certificates;
    }

    public static PrivateKey loadPrivateKey(File privateKeyFile, Optional<String> keyPassword)
            throws IOException, GeneralSecurityException
    {
        String privateKey = asCharSource(privateKeyFile, US_ASCII).read();
        return loadPrivateKey(privateKey, keyPassword);
    }

    public static PrivateKey loadPrivateKey(String privateKey, Optional<String> keyPassword)
            throws IOException, GeneralSecurityException
    {
        Matcher matcher = PRIVATE_KEY_PATTERN.matcher(privateKey);
        if (!matcher.find()) {
            throw new KeyStoreException("did not find a private key");
        }
        String keyType = matcher.group(1);
        String base64Key = matcher.group(2);

        if (base64Key.toLowerCase(US).startsWith("proc-type")) {
            throw new InvalidKeySpecException("Password protected PKCS 1 private keys are not supported");
        }

        byte[] encodedKey = base64Decode(base64Key);

        PKCS8EncodedKeySpec encodedKeySpec;
        if (keyType == null) {
            encodedKeySpec = new PKCS8EncodedKeySpec(encodedKey);
        }
        else if ("ENCRYPTED".equals(keyType)) {
            if (!keyPassword.isPresent()) {
                throw new KeyStoreException("Private key is encrypted, but no password was provided");
            }
            EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = new EncryptedPrivateKeyInfo(encodedKey);
            SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(encryptedPrivateKeyInfo.getAlgName());
            SecretKey secretKey = keyFactory.generateSecret(new PBEKeySpec(keyPassword.get().toCharArray()));

            Cipher cipher = Cipher.getInstance(encryptedPrivateKeyInfo.getAlgName());
            cipher.init(DECRYPT_MODE, secretKey, encryptedPrivateKeyInfo.getAlgParameters());

            encodedKeySpec = encryptedPrivateKeyInfo.getKeySpec(cipher);
        }
        else {
            return loadPkcs1PrivateKey(keyType, encodedKey);
        }

        // this code requires a key in PKCS8 format which is not the default openssl format
        // to convert to the PKCS8 format you use : openssl pkcs8 -topk8 ...
        Set<String> algorithms = ImmutableSet.of("RSA", "EC", "DSA");
        for (String algorithm : algorithms) {
            try {
                KeyFactory keyFactory = KeyFactory.getInstance(algorithm);
                return keyFactory.generatePrivate(encodedKeySpec);
            }
            catch (InvalidKeySpecException ignore) {
            }
        }
        throw new InvalidKeySpecException("Key type must be one of " + algorithms);
    }

    private static PrivateKey loadPkcs1PrivateKey(String pkcs1KeyType, byte[] pkcs1Key)
            throws GeneralSecurityException
    {
        byte[] pkcs8Key;
        switch (pkcs1KeyType) {
            case "RSA":
                pkcs8Key = rsaPkcs1ToPkcs8(pkcs1Key);
                break;
            case "DSA":
                pkcs8Key = dsaPkcs1ToPkcs8(pkcs1Key);
                break;
            case "EC":
                pkcs8Key = ecPkcs1ToPkcs8(pkcs1Key);
                break;
            default:
                throw new InvalidKeySpecException(pkcs1KeyType + " private key in PKCS 1 format is not supported");
        }
        try {
            KeyFactory keyFactory = KeyFactory.getInstance(pkcs1KeyType);
            return keyFactory.generatePrivate(new PKCS8EncodedKeySpec(pkcs8Key));
        }
        catch (InvalidKeySpecException e) {
            throw new InvalidKeySpecException(format("Invalid %s private key in PKCS 1 format", pkcs1KeyType), e);
        }
    }

    static byte[] rsaPkcs1ToPkcs8(byte[] pkcs1)
    {
        byte[] keyIdentifier = encodeSequence(RSA_KEY_OID, DER_NULL);
        return encodeSequence(VERSION_0_ENCODED, keyIdentifier, encodeOctetString(pkcs1));
    }

    static byte[] dsaPkcs1ToPkcs8(byte[] pkcs1)
            throws InvalidKeySpecException
    {
        List<byte[]> elements = decodeSequence(pkcs1);
        if (elements.size() != 6) {
            throw new InvalidKeySpecException("Expected DSA key to have 6 elements");
        }
        byte[] keyIdentifier = encodeSequence(DSA_KEY_OID, encodeSequence(elements.get(1), elements.get(2), elements.get(3)));
        return encodeSequence(VERSION_0_ENCODED, keyIdentifier, encodeOctetString(elements.get(5)));
    }

    static byte[] ecPkcs1ToPkcs8(byte[] pkcs1)
            throws InvalidKeySpecException
    {
        List<byte[]> elements = decodeSequence(pkcs1);
        if (elements.size() != 4) {
            throw new InvalidKeySpecException("Expected EC key to have 4 elements");
        }
        byte[] curveOid = decodeSequenceOptionalElement(elements.get(2));
        byte[] keyIdentifier = encodeSequence(EC_KEY_OID, curveOid);
        return encodeSequence(VERSION_0_ENCODED, keyIdentifier, encodeOctetString(encodeSequence(elements.get(0), elements.get(1), elements.get(3))));
    }

    private static boolean matches(PrivateKey privateKey, Certificate certificate)
    {
        try {
            PublicKey publicKey = certificate.getPublicKey();

            Signature signer = createSignature(privateKey, publicKey);

            signer.initSign(privateKey);
            signer.update(TEST_SIGNATURE_DATA);
            byte[] signature = signer.sign();

            signer.initVerify(publicKey);
            signer.update(TEST_SIGNATURE_DATA);
            return signer.verify(signature);
        }
        catch (GeneralSecurityException ignored) {
            return false;
        }
    }

    private static Signature createSignature(PrivateKey privateKey, PublicKey publicKey)
            throws GeneralSecurityException
    {
        if (privateKey instanceof RSAPrivateKey && publicKey instanceof RSAPublicKey) {
            return Signature.getInstance("NONEwithRSA");
        }
        if (privateKey instanceof ECPrivateKey && publicKey instanceof ECPublicKey) {
            return Signature.getInstance("NONEwithECDSA");
        }
        if (privateKey instanceof DSAKey && publicKey instanceof DSAKey) {
            return Signature.getInstance("NONEwithDSA");
        }
        throw new InvalidKeySpecException("Key type must be one of " + SUPPORTED_KEY_TYPES);
    }

    public static byte[] base64Decode(String base64)
    {
        return getMimeDecoder().decode(base64.getBytes(US_ASCII));
    }
}
