/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */

package com.starburstdata.trino.plugins.snowflake.distributed;

import com.microsoft.azure.keyvault.cryptography.ICryptoTransform;
import com.microsoft.azure.keyvault.cryptography.algorithms.AesKw;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;

import static java.util.Objects.requireNonNull;

public final class AesKwPkcs5
        extends AesKw
{
    public static final String ALGORITHM_NAME = "AES_CBC_256";

    private static final String AES = "AES";
    private static final String KEY_CIPHER = "AES/ECB/PKCS5Padding";

    public AesKwPkcs5()
    {
        super(ALGORITHM_NAME);
    }

    @Override
    public ICryptoTransform CreateEncryptor(byte[] key, byte[] iv, Provider provider)
            throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException
    {
        return getKeyWrappingTransform(Cipher.ENCRYPT_MODE, key);
    }

    @Override
    public ICryptoTransform CreateDecryptor(byte[] key, byte[] iv, Provider provider)
            throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException
    {
        return getKeyWrappingTransform(Cipher.DECRYPT_MODE, key);
    }

    private ICryptoTransform getKeyWrappingTransform(int cipherMode, byte[] key)
            throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException
    {
        requireNonNull(key, "key must not be null");
        final Cipher keyCipher = Cipher.getInstance(KEY_CIPHER);
        SecretKey queryStageMasterKey = new SecretKeySpec(key, 0, key.length, AES);
        // IV is used only for decrypting/encrypting blob contents, not the key. Details of the reference implementation can be found in:
        // https://github.com/snowflakedb/snowflake-jdbc/blob/v3.12.8/src/main/java/net/snowflake/client/jdbc/cloud/storage/EncryptionProvider.java#L111
        // IV will be used later by BlobEncryptionPolicy#decryptBlob
        keyCipher.init(cipherMode, queryStageMasterKey);
        return keyCipher::doFinal;
    }
}
