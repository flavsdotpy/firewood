package com.ap3x.firewood.helpers;

import com.ap3x.firewood.exceptions.CryptographyException;
import com.ap3x.firewood.services.other.VaultService;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.vault.support.VaultResponse;

import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;

@Component
public class KeyGeneratorHelper {
    public static final int RSA_KEY_LENGTH = 2048;
    private RSAPrivateKey privateKey;
    private RSAPublicKey publicKey;

    @Autowired
    private VaultService vaultService;

    public String verifyKey(String fieldName, String token) {
        try {
            return this.verifyKey(fieldName, "priv", token);
        } catch (CryptographyException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String verifyKey(String fieldName, String keyType, String token) throws CryptographyException {
        String existingPublicKey = this.getKey(fieldName, keyType, token);
        if (existingPublicKey != null)
            return existingPublicKey;
        else {
            postKey(fieldName, token);
            String content = this.getKey(fieldName, keyType, token);
            if (content != null)
                return content;
            else
                throw new CryptographyException(new Exception("Key doesn't exists and failed when post the private and public key in vault"));
        }

    }

    private String getKey(String fieldName, String keyType, String token) {
        try {
            JSONObject vaultResponse = new JSONObject(get(fieldName, keyType, token));
            return vaultResponse.getJSONObject("data").getString("content");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private boolean postKey(String fieldName, String token) {
        this.generateKeyPair(RSA_KEY_LENGTH);
        return this.postIntoVault(fieldName, token);
    }

    private boolean postIntoVault(String fieldName, String token) {
        try {
            boolean priv = post(fieldName, "priv", this.privateKey, token);
            boolean pub = post(fieldName, "pub", this.publicKey, token);
            if (priv && pub) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public String getPub(String fieldName, String token) {
        return this.getKey(fieldName, "pub", token);
    }

    public String getPriv(String fieldName, String token) {
        return this.getKey(fieldName, "priv", token);
    }

    private VaultResponse get(String fieldName, String keyType, String token) {

        try {

            VaultResponse response = vaultService.readSecrets(fieldName, keyType, token);

            if (response != null) {
                return response;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private boolean post(String fieldName, String keyType, Key key, String token) {

        try {
            vaultService.writeSecrets(fieldName, keyType, Base64.getEncoder().encodeToString(key.getEncoded()), token);
            VaultResponse response = vaultService.readSecrets(fieldName, keyType, token);
            if (response != null)
                return true;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public KeyPair generateKeyPair(int keySize) {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(keySize);
            KeyPair keyPair = generator.generateKeyPair();
            this.privateKey = (RSAPrivateKey) keyPair.getPrivate();
            this.publicKey = (RSAPublicKey) keyPair.getPublic();
            return keyPair;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }

    private String getPublicKey() {
        return Base64.getEncoder().encodeToString(this.publicKey.getEncoded());
    }

    private String getPrivateKey() {
        return Base64.getEncoder().encodeToString(this.privateKey.getEncoded());
    }

}