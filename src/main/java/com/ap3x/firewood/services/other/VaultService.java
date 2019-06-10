package com.ap3x.firewood.services.other;

import com.google.common.annotations.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.vault.authentication.TokenAuthentication;
import org.springframework.vault.client.VaultEndpoint;
import org.springframework.vault.core.VaultOperations;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.support.VaultResponse;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Component
public class VaultService {

    @Autowired
    private Environment env;

    private VaultOperations operations;

    @VisibleForTesting
    void setToken(String token) {
        this.operations = new VaultTemplate(VaultEndpoint.from(URI.create(env.getProperty("vault.uri"))), new TokenAuthentication(token));
    }

    public VaultResponse writeSecrets(String fieldName, String keyType, String key, String token) {
        this.setToken(token);
        Map<String, String> data = new HashMap<String, String>();
        data.put("content", key);

        VaultResponse response = this.operations.write(this.buildURI(keyType, fieldName), data);

        return response;
    }

    public VaultResponse readSecrets(String fieldName, String keyType, String token) {
        this.setToken(token);
        VaultResponse response = this.operations.read(this.buildURI(keyType, fieldName));
        return response;
    }

    private String buildURI(String keyType, String fieldName) {
        String path = "secret/data-security/";
        return path + fieldName.toLowerCase() + "/" + keyType.toLowerCase();
    }
}

