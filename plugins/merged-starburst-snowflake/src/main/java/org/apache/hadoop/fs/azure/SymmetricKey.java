/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package org.apache.hadoop.fs.azure;

import com.microsoft.azure.keyvault.cryptography.Algorithm;
import com.microsoft.azure.keyvault.cryptography.AlgorithmResolver;
import com.microsoft.azure.keyvault.cryptography.IAuthenticatedCryptoTransform;
import com.microsoft.azure.keyvault.cryptography.ICryptoTransform;
import com.microsoft.azure.keyvault.cryptography.KeyWrapAlgorithm;
import com.microsoft.azure.keyvault.cryptography.Strings;
import com.microsoft.azure.keyvault.cryptography.SymmetricEncryptionAlgorithm;
import com.microsoft.azure.keyvault.cryptography.algorithms.Aes128Cbc;
import com.microsoft.azure.keyvault.cryptography.algorithms.Aes128CbcHmacSha256;
import com.microsoft.azure.keyvault.cryptography.algorithms.Aes192Cbc;
import com.microsoft.azure.keyvault.cryptography.algorithms.Aes192CbcHmacSha384;
import com.microsoft.azure.keyvault.cryptography.algorithms.Aes256CbcHmacSha512;
import com.microsoft.azure.keyvault.cryptography.algorithms.AesKw128;
import com.microsoft.azure.keyvault.cryptography.algorithms.AesKw192;
import com.microsoft.azure.keyvault.cryptography.algorithms.AesKw256;
import com.starburstdata.presto.plugin.snowflake.distributed.AesKwPkcs5;
import io.trino.hadoop.$internal.com.google.common.util.concurrent.Futures;
import io.trino.hadoop.$internal.com.google.common.util.concurrent.ListenableFuture;
import io.trino.hadoop.$internal.com.microsoft.azure.keyvault.core.IKey;
import io.trino.hadoop.$internal.org.apache.commons.lang3.NotImplementedException;
import io.trino.hadoop.$internal.org.apache.commons.lang3.tuple.Pair;
import io.trino.hadoop.$internal.org.apache.commons.lang3.tuple.Triple;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.SecureRandom;
import java.util.UUID;

/**
 * A simple symmetric key implementation
 *
 * Copied from com.microsoft.azure.keyvault.cryptography.SymmetricKey from package com.microsoft.azure:azure-keyvault-cryptography:1.0.0
 */
public class SymmetricKey
        implements IKey {

    private static final SecureRandom Rng = new SecureRandom();

    public static final int KeySize128 = 128 >> 3;
    public static final int KeySize192 = 192 >> 3;
    public static final int KeySize256 = 256 >> 3;
    public static final int KeySize384 = 384 >> 3;
    public static final int KeySize512 = 512 >> 3;
    
    public static final int DefaultKeySize = KeySize256;
    public static final AesKwPkcs5 SNOWFLAKE_KW_ALGORITHM = new AesKwPkcs5();

    private final String   _kid;
    private final byte[]   _key;
    private final Provider _provider;
    
    /**
     * Creates a SymmetricKey with a random key identifier and
     * a random key with DefaultKeySize bits.
     */
    public SymmetricKey() {
        this(UUID.randomUUID().toString());
    }
    
    /**
     * Creates a SymmetricKey with the specified key identifier and
     * a random key with DefaultKeySize bits.
     * @param kid
     *      The key identifier to use.
     */
    public SymmetricKey(String kid) {
        this(kid, DefaultKeySize);
    }
    
    /**
     * Creates a SymmetricKey with the specified key identifier and
     * a random key with the specified size.
     * @param kid
     *      The key identifier to use.
     * @param keySizeInBytes
     *      The key size to use in bytes.
     */
    public SymmetricKey(String kid, int keySizeInBytes ) {
        this(kid, keySizeInBytes, null);
    }
    
    /**
     * Creates a SymmetricKey with the specified key identifier and
     * a random key with the specified size that uses the specified provider.
     * @param kid
     *      The key identifier to use.
     * @param keySizeInBytes
     *      The key size to use in bytes.
     * @param provider
     *      The provider to use (optional, null for default)
     */
    public SymmetricKey(String kid, int keySizeInBytes, Provider provider) {
        
        if ( Strings.isNullOrWhiteSpace(kid) ) {
            throw new IllegalArgumentException("kid");
        }
        
        if ( keySizeInBytes != KeySize128 && keySizeInBytes != KeySize192 && keySizeInBytes != KeySize256 && keySizeInBytes != KeySize384 && keySizeInBytes != KeySize512 ) {
            throw new IllegalArgumentException("The key material must be 128, 192, 256, 384 or 512 bits of data");
        }
        
        _kid      = kid;
        _key      = new byte[keySizeInBytes];
        _provider = provider;
        
        // Generate a random key
        Rng.nextBytes(_key);
    }

    /**
     * Creates a SymmetricKey with the specified key identifier and key material.
     * @param kid
     *      The key identifier to use.
     * @param keyBytes
     *      The key material to use.
     */
    public SymmetricKey(String kid, byte[] keyBytes) {
        this(kid, keyBytes, null);
    }

    /**
     * Creates a SymmetricKey with the specified key identifier and key material
     * that uses the specified Provider.
     * @param kid
     *      The key identifier to use.
     * @param keyBytes
     *      The key material to use.
     * @param provider
     *      The Provider to use (optional, null for default)
     */
    public SymmetricKey(String kid, byte[] keyBytes, Provider provider) {

        if ( Strings.isNullOrWhiteSpace(kid) ) {
            throw new IllegalArgumentException("kid");
        }

        if ( keyBytes == null ) {
            throw new IllegalArgumentException("keyBytes");
        }

        if ( keyBytes.length != KeySize128 && keyBytes.length != KeySize192 && keyBytes.length != KeySize256 && keyBytes.length != KeySize384 && keyBytes.length != KeySize512 ) {
            throw new IllegalArgumentException("The key material must be 128, 192, 256, 384 or 512 bits of data");
        }

        _kid      = kid;
        _key      = keyBytes;
        _provider = provider;
    }

    @Override
    public String getDefaultEncryptionAlgorithm() {

        switch (_key.length) {
        case KeySize128:
            return Aes128Cbc.ALGORITHM_NAME;

        case KeySize192:
            return Aes192Cbc.ALGORITHM_NAME;

        case KeySize256:
            return Aes128CbcHmacSha256.ALGORITHM_NAME;

        case KeySize384:
            return Aes192CbcHmacSha384.ALGORITHM_NAME;

        case KeySize512:
            return Aes256CbcHmacSha512.ALGORITHM_NAME;
        }

        return null;
    }

    @Override
    public String getDefaultKeyWrapAlgorithm() {

        switch (_key.length) {
        case KeySize128:
            return AesKw128.ALGORITHM_NAME;

        case KeySize192:
            return AesKw192.ALGORITHM_NAME;

        case KeySize256:
            return AesKw256.ALGORITHM_NAME;

        case KeySize384:
            // Default to longest allowed key length for wrap
            return AesKw256.ALGORITHM_NAME;

        case KeySize512:
            // Default to longest allowed key length for wrap
            return AesKw256.ALGORITHM_NAME;
        }

        return null;
    }

    @Override
    public String getDefaultSignatureAlgorithm() {

        return null;
    }

    @Override
    public String getKid() {

        return _kid;
    }

    @Override
    public ListenableFuture<byte[]> decryptAsync(final byte[] ciphertext, final byte[] iv, final byte[] authenticationData, final byte[] authenticationTag, final String algorithm) throws NoSuchAlgorithmException {

        if (Strings.isNullOrWhiteSpace(algorithm)) {
            throw new IllegalArgumentException("algorithm");
        }

        if (ciphertext == null) {
            throw new IllegalArgumentException("ciphertext");
        }

        if (iv == null) {
            throw new IllegalArgumentException("iv");
        }

        // Interpret the algorithm
        Algorithm baseAlgorithm = AlgorithmResolver.Default.get(algorithm);

        if (baseAlgorithm == null || !(baseAlgorithm instanceof SymmetricEncryptionAlgorithm)) {
            throw new NoSuchAlgorithmException(algorithm);
        }

        SymmetricEncryptionAlgorithm algo = (SymmetricEncryptionAlgorithm)baseAlgorithm;

        ICryptoTransform transform = null;

        try {
            transform = algo.CreateDecryptor(_key, iv, authenticationData, authenticationTag, _provider );
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }

        byte[] result = null;

        try {
            result = transform.doFinal(ciphertext);
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }

        return Futures.immediateFuture(result);
    }

    @Override
    public ListenableFuture<Triple<byte[], byte[], String>> encryptAsync(final byte[] plaintext, final byte[] iv, final byte[] authenticationData, final String algorithm) throws NoSuchAlgorithmException {

        if (plaintext == null) {
            throw new IllegalArgumentException("plaintext");
        }

        if (iv == null) {
            throw new IllegalArgumentException("iv");
        }

        // Interpret the algorithm
        String    algorithmName = Strings.isNullOrWhiteSpace(algorithm) ? getDefaultEncryptionAlgorithm() : algorithm;
        Algorithm baseAlgorithm = AlgorithmResolver.Default.get(algorithmName);
        
        if (baseAlgorithm == null || !(baseAlgorithm instanceof SymmetricEncryptionAlgorithm)) {
            throw new NoSuchAlgorithmException(algorithm);
        }
        
        SymmetricEncryptionAlgorithm algo = (SymmetricEncryptionAlgorithm)baseAlgorithm;

        ICryptoTransform transform = null;

        try {
            transform = algo.CreateEncryptor(_key, iv, authenticationData, _provider);
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }

        byte[] cipherText = null;

        try {
            cipherText = transform.doFinal(plaintext);
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }

        byte[] authenticationTag = null;

        if (transform instanceof IAuthenticatedCryptoTransform) {

            IAuthenticatedCryptoTransform authenticatedTransform = (IAuthenticatedCryptoTransform) transform;

            authenticationTag = authenticatedTransform.getTag().clone();
        }

        return Futures.immediateFuture(Triple.of(cipherText, authenticationTag, algorithm));
    }

    @Override
    public ListenableFuture<Pair<byte[], String>> wrapKeyAsync(final byte[] key, final String algorithm) throws NoSuchAlgorithmException {

        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("key");
        }

        // Interpret the algorithm
        String    algorithmName = Strings.isNullOrWhiteSpace(algorithm) ? getDefaultKeyWrapAlgorithm() : algorithm;
        Algorithm baseAlgorithm = AlgorithmResolver.Default.get(algorithmName);
        
        if (baseAlgorithm == null || !(baseAlgorithm instanceof KeyWrapAlgorithm)) {
            throw new NoSuchAlgorithmException(algorithmName);
        }
        
        KeyWrapAlgorithm algo = (KeyWrapAlgorithm)baseAlgorithm;

        ICryptoTransform transform = null;

        try {
            transform = algo.CreateEncryptor(_key, null, _provider);
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }

        byte[] encrypted = null;

        try {
            encrypted = transform.doFinal(key);
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }

        return Futures.immediateFuture(Pair.of(encrypted, algorithmName));
    }

    @Override
    public ListenableFuture<byte[]> unwrapKeyAsync(final byte[] encryptedKey, final String algorithm) throws NoSuchAlgorithmException {

        if (Strings.isNullOrWhiteSpace(algorithm)) {
            throw new IllegalArgumentException("algorithm");
        }

        if (encryptedKey == null || encryptedKey.length == 0) {
            throw new IllegalArgumentException("wrappedKey");
        }

        Algorithm baseAlgorithm = null;
        if (algorithm.equals(AesKwPkcs5.ALGORITHM_NAME)) {
            baseAlgorithm = SNOWFLAKE_KW_ALGORITHM;
        } else {
            baseAlgorithm = AlgorithmResolver.Default.get(algorithm);
        }

        if (baseAlgorithm == null || !(baseAlgorithm instanceof KeyWrapAlgorithm)) {
            throw new NoSuchAlgorithmException(algorithm);
        }

        KeyWrapAlgorithm algo = (KeyWrapAlgorithm)baseAlgorithm;

        ICryptoTransform transform = null;

        try {
            transform = algo.CreateDecryptor(_key, null, _provider);
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }

        byte[] decrypted = null;

        try {
            decrypted = transform.doFinal(encryptedKey);
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }

        return Futures.immediateFuture(decrypted);
    }

    @Override
    public ListenableFuture<Pair<byte[], String>> signAsync(final byte[] digest, final String algorithm) {
        return Futures.immediateFailedFuture(new NotImplementedException("signAsync is not currently supported"));
    }

    @Override
    public ListenableFuture<Boolean> verifyAsync(final byte[] digest, final byte[] signature, final String algorithm) {
        return Futures.immediateFailedFuture(new NotImplementedException("verifyAsync is not currently supported"));
    }

    @Override
    public void close() throws IOException {
    }
}
