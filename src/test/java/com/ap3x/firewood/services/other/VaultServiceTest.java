package com.ap3x.firewood.services.other;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.vault.core.VaultOperations;
import org.springframework.vault.support.VaultResponse;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.Mockito.when;

public class VaultServiceTest {

    @Mock
    private VaultOperations operations;

    @InjectMocks
    private VaultService vaultService;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void readSecretsTest() {
        VaultResponse vaultResponse = getVaultResponse("some-pub-key");
        when(this.operations.read("secret/data-security/cpf/pub")).thenReturn(vaultResponse);

        setVaultService();

        VaultResponse response = vaultService.readSecrets("cpf", "pub", "vaultinho");
        Assert.assertEquals(response.getData().get("content"), "some-pub-key");

    }

    @Test
    public void writeSecrets() {
        Map<String, String> data = new HashMap<String, String>();
        data.put("content", "some-priv-key");

        VaultResponse vaultResponse = getVaultResponse("some-priv-key");
        when(this.operations.write("secret/data-security/cpf/priv", data)).thenReturn(vaultResponse);

        setVaultService();

        VaultResponse response = vaultService.writeSecrets("cpf", "priv", "some-priv-key", "vaultinho");
        Assert.assertEquals(response.getData().get("content"), "some-priv-key");
    }

    private void setVaultService() {
        vaultService = Mockito.spy(vaultService);

        try {
            Mockito.doNothing().when(vaultService).setToken("vaultinho");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private VaultResponse getVaultResponse(String value) {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("content", value);

        VaultResponse vaultResponse = new VaultResponse();
        vaultResponse.setData(data);

        return vaultResponse;
    }
}