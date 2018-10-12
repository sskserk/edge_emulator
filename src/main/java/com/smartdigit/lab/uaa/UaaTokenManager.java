package com.smartdigit.lab.uaa;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Base64;

public class UaaTokenManager {
    private static final Logger logger = LoggerFactory.getLogger(UaaTokenManager.class.getName());

    private final String uaaObtainUrl;

    public UaaTokenManager(String uaaObtainUrl) {
        this.uaaObtainUrl = uaaObtainUrl;
    }

    public String obtainToken(String clientId, String clientSecret) throws IOException {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(this.uaaObtainUrl);
        httpPost.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

        String encodedCreds = new String(Base64.getEncoder().encode((clientId + ":" + clientSecret).getBytes()));
        httpPost.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedCreds);
        logger.debug("Generated base64 sequence {}", encodedCreds);

        try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
            String jsonString = IOUtils.toString(response.getEntity().getContent());
            logger.debug("response: {}", jsonString);

            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(jsonString);

            String token = rootNode.get("access_token").asText();
            logger.info("Obtained access token: {}", token);

            return token;
        }
    }
}
