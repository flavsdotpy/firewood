package com.ap3x.firewood.helpers;

import com.ap3x.firewood.exceptions.CryptographyException;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.security.*;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

@Component
public class RsaHelper {

    @Autowired
    private KeyGeneratorHelper keyGeneratorHelper;

    public RsaHelper() {
        Security.addProvider(new BouncyCastleProvider());
    }

    public RSAPrivateKey convertB64toPKCS8(String priv) throws CryptographyException {
        try {
            KeyFactory keyFactory = KeyFactory.getInstance("RSA", "BC");
            PKCS8EncodedKeySpec pkcs8EncodedKeySpec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(priv));
            return (RSAPrivateKey) keyFactory.generatePrivate(pkcs8EncodedKeySpec);
        } catch (NoSuchAlgorithmException | NoSuchProviderException | IllegalArgumentException | InvalidKeySpecException e) {
            throw new CryptographyException(e);
        }
    }

    public RSAPublicKey convertB64toX509(String pub) throws CryptographyException {
        try {
            KeyFactory keyFactory = KeyFactory.getInstance("RSA", "BC");
            X509EncodedKeySpec x509EncodedKeySpec = new X509EncodedKeySpec(Base64.getDecoder().decode(pub));
            return (RSAPublicKey) keyFactory.generatePublic(x509EncodedKeySpec);
        } catch (NoSuchAlgorithmException | NoSuchProviderException | IllegalArgumentException | InvalidKeySpecException e) {
            throw new CryptographyException(e);
        }
    }

    private Cipher getCipherEncrypt(RSAPublicKey publicKey) throws CryptographyException {
        Cipher cipher;
        try {
            cipher = Cipher.getInstance("RSA/NONE/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, publicKey);
            return cipher;
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
            throw new CryptographyException(e);
        }
    }

    private Cipher getCipherDecrypt(RSAPrivateKey privateKey) throws CryptographyException {
        Cipher cipher;
        try {
            cipher = Cipher.getInstance("RSA/NONE/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, privateKey);
            return cipher;
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
            throw new CryptographyException(e);
        }
    }

    public String encrypt(String pub, String plainText) throws CryptographyException {
        RSAPublicKey publicKey = convertB64toX509(pub);
        Cipher cipher = getCipherEncrypt(publicKey);
        try {
            byte[] encryptedbytes = cipher.doFinal(plainText.getBytes());
            return Base64.getEncoder().encodeToString(encryptedbytes);
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new CryptographyException(e);
        }
    }

    public String decrypt(String priv, String encodedByteArray) throws CryptographyException {
        RSAPrivateKey privateKey = convertB64toPKCS8(priv);
        Cipher cipher = getCipherDecrypt(privateKey);
        try {
            byte[] decrypted = cipher.doFinal(Base64.getDecoder().decode(encodedByteArray));
            return new String(decrypted);
        } catch (IllegalBlockSizeException | BadPaddingException | IllegalArgumentException e) {
            throw new CryptographyException(e);
        }
    }
}