package com.ap3x.firewood.helpers;

import com.ap3x.firewood.exceptions.CryptographyException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.KeyPair;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;

public class RsaHelperTest {
    private RSAPrivateKey privateKey;
    private RSAPublicKey publicKey;
    private RsaHelper rsaUtils;
    private String plainText = "Desta maneira, a execução dos pontos do programa maximiza as possibilidades por conta do retorno esperado a longo prazo.";

    @Before
    public void setUp() {
        KeyGeneratorHelper keyGenerator = new KeyGeneratorHelper();
        KeyPair keyPair = keyGenerator.generateKeyPair(2048);
        this.privateKey = (RSAPrivateKey) keyPair.getPrivate();
        this.publicKey = (RSAPublicKey) keyPair.getPublic();
        this.rsaUtils = new RsaHelper();
    }

    @Test
    public void encryptDecryptTest() {
        String encrypted = null;
        try {
            encrypted = rsaUtils.encrypt(Base64.getEncoder().encodeToString(publicKey.getEncoded()), this.plainText);
            String decrypted = rsaUtils.decrypt(Base64.getEncoder().encodeToString(privateKey.getEncoded()), encrypted);
            Assert.assertEquals(this.plainText, decrypted);
        } catch (CryptographyException e) {
            e.printStackTrace();
        }


    }

    @Test
    public void convertDeconvertKeysTest() {
        String b64convertedPriv = Base64.getEncoder().encodeToString(privateKey.getEncoded());
        String b64convertedPub = Base64.getEncoder().encodeToString(publicKey.getEncoded());

        try {
            Assert.assertEquals(this.privateKey, rsaUtils.convertB64toPKCS8(b64convertedPriv));
        } catch (CryptographyException e) {
            e.printStackTrace();
        }
        try {
            Assert.assertEquals(this.publicKey, rsaUtils.convertB64toX509(b64convertedPub));
        } catch (CryptographyException e) {
            e.printStackTrace();
        }
    }
}
