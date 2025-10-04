package com.google.cloud.kafka;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredJws;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.IOException;
import java.util.*;

/**
 * Custom OAuth Bearer Callback Handler for Google Cloud Managed Kafka
 * This handler retrieves Google Cloud credentials and creates OAuth tokens
 */
public class GoogleCloudOAuthBearerCallbackHandler implements AuthenticateCallbackHandler {
    
    private GoogleCredentials credentials;
    
    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        try {
            // Get Application Default Credentials (ADC)
            this.credentials = GoogleCredentials.getApplicationDefault()
                .createScoped(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
            
            System.out.println("Google Cloud credentials loaded successfully");
            if (credentials instanceof ServiceAccountCredentials) {
                ServiceAccountCredentials saCredentials = (ServiceAccountCredentials) credentials;
                System.out.println("   Service Account: " + saCredentials.getServiceAccountUser());
            }
        } catch (IOException e) {
            System.err.println("Failed to load Google Cloud credentials: " + e.getMessage());
            throw new RuntimeException("Failed to load Google Cloud credentials", e);
        }
    }
    
    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback) {
                handleTokenCallback((OAuthBearerTokenCallback) callback);
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }
    
    private void handleTokenCallback(OAuthBearerTokenCallback callback) {
        try {
            // Refresh credentials if needed
            if (credentials.getAccessToken() == null || credentials.getAccessToken().getTokenValue() == null) {
                credentials.refresh();
            }
            
            // Get the access token
            String accessToken = credentials.getAccessToken().getTokenValue();
            Date expirationTime = credentials.getAccessToken().getExpirationTime();
            
            // Get service account email for principal
            String principalName = "google-cloud-service-account";
            if (credentials instanceof ServiceAccountCredentials) {
                ServiceAccountCredentials saCredentials = (ServiceAccountCredentials) credentials;
                principalName = saCredentials.getServiceAccountUser();
            }
            
            // Create JWT token string
            long now = System.currentTimeMillis();
            long expiry = expirationTime != null ? expirationTime.getTime() : now + 3600000; // 1 hour default
            
            // Create an unsecured JWT with "none" algorithm as required by Kafka
            String compactSerialization = createUnsecuredJwtToken(principalName, accessToken, now, expiry);
            
            // Create OAuth Bearer Token using the unsecured JWT
            OAuthBearerToken token = new OAuthBearerUnsecuredJws(compactSerialization, principalName, "kafka");
            
            callback.token(token);
            System.out.println("OAuth token generated for principal: " + principalName);
            
        } catch (IOException e) {
            System.err.println("Failed to get OAuth token: " + e.getMessage());
            callback.error("invalid_grant", e.getMessage(), null);
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
            callback.error("server_error", e.getMessage(), null);
        }
    }
    
    /**
     * Create an unsecured JWT token with algorithm "none" as required by Kafka's OAuthBearerUnsecuredJws
     * We embed the Google Cloud access token in the payload as an extension
     */
    private String createUnsecuredJwtToken(String principalName, String accessToken, long iat, long exp) {
        // Header must have "none" algorithm for unsecured JWS
        String header = "{\"typ\":\"JWT\",\"alg\":\"none\"}";
        
        // Payload with required claims and Google Cloud token as extension
        String payload = String.format(
            "{\"sub\":\"%s\",\"iat\":%d,\"exp\":%d,\"google_access_token\":\"%s\"}",
            principalName,
            iat / 1000,  // Convert to seconds
            exp / 1000,  // Convert to seconds
            accessToken  // Store Google access token as a claim
        );
        
        // Encode to base64url
        String encodedHeader = base64UrlEncode(header);
        String encodedPayload = base64UrlEncode(payload);
        
        // For unsecured JWT, the signature is empty
        return encodedHeader + "." + encodedPayload + ".";
    }
    
    /**
     * Base64 URL-safe encoding without padding
     */
    private String base64UrlEncode(String data) {
        return Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(data.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
    
    @Override
    public void close() {
        // No resources to close
    }
}
