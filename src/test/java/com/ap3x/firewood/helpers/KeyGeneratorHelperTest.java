package com.ap3x.firewood.helpers;

import com.ap3x.firewood.services.other.VaultService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.vault.support.VaultResponse;

import java.security.KeyPair;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.Mockito.when;

public class KeyGeneratorHelperTest {

    @Mock
    private VaultService vaultHelper;

    @InjectMocks
    private KeyGeneratorHelper keyGenerator;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void verifyKeyTest() throws Exception {
        VaultResponse vaultResponse = getVaultResponse("some-pub-key");

        when(vaultHelper.readSecrets("password", "pub", "vaultinho")).thenReturn(vaultResponse);

        String resp1 = keyGenerator.verifyKey("password", "pub", "vaultinho");
        Assert.assertEquals(resp1, "some-pub-key");

        vaultResponse = getVaultResponse("some-priv-key");
        when(vaultHelper.readSecrets("password", "priv", "vaultinho")).thenReturn(vaultResponse);
        String resp2 = keyGenerator.verifyKey("password", "vaultinho");
        Assert.assertEquals(resp2, "some-priv-key");
    }

    @Test
    public void getTest() {
        VaultResponse vaultResponse = getVaultResponse("some-pub-key");

        when(vaultHelper.readSecrets("password", "pub", "vaultinho")).thenReturn(vaultResponse);

        String resp = keyGenerator.getPub("password", "vaultinho");
        Assert.assertEquals(resp, "some-pub-key");
    }

    @Test
    public void generateKeyPairTest() {
        KeyPair keyPair = keyGenerator.generateKeyPair(2048);

        Assert.assertEquals(keyPair.getClass(), KeyPair.class);
        Assert.assertNotNull(keyPair);

        RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();
        RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();

        Assert.assertEquals(privateKey.getClass(), sun.security.rsa.RSAPrivateCrtKeyImpl.class);
        Assert.assertEquals(publicKey.getClass(), sun.security.rsa.RSAPublicKeyImpl.class);

        Assert.assertNotNull(privateKey);
        Assert.assertNotNull(publicKey);

    }

    private VaultResponse getVaultResponse(String value) {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("content", value);

        VaultResponse vaultResponse = new VaultResponse();
        vaultResponse.setData(data);

        return vaultResponse;
    }
}