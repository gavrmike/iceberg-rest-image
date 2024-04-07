package org.apache.iceberg.rest;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JWebToken {

    private static final Logger LOG = LoggerFactory.getLogger(JWebToken.class);

    private static final String ISSUER = "org.apache.iceberg";
    private static final String JWT_HEADER = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";

    private ObjectNode payload = null;
    private String signature = null;
    private String encodedHeader = null;

    public static class JWTHeader {
        public String alg;
        public String typ;
    }

    private JWebToken() throws JsonMappingException, JsonProcessingException {
        JWTHeader headerObject = RESTObjectMapper.mapper().readValue(JWT_HEADER, JWTHeader.class);
        encodedHeader = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(RESTObjectMapper.mapper().writeValueAsBytes(headerObject));
    }

    public JWebToken(ObjectNode payload, String secretKey) throws JsonMappingException, JsonProcessingException {
        this(payload.get("sub").asText(), payload.get("aud"), payload.get("exp").asLong(), secretKey);
    }

    public JWebToken(String sub, long expires, String secretKey) throws JsonMappingException, JsonProcessingException {
        this(sub, new ArrayNode(RESTObjectMapper.mapper().getNodeFactory()), expires, secretKey);
    }

    public static String buildToken(String sub, String secret) {
        try {
            long expires = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) + 10 * 86400 * 365;
            JWebToken jwt = new JWebToken(sub, expires, secret);
            jwt.validate(x -> secret);
            return jwt.toString();
        } catch (JsonProcessingException | RuntimeException e) {
            throw new RuntimeException(e);
        }
    }

    public static String extractUserFromToken(String token, Function<String, String> getSecret) {
        JWebToken jwt;
        try {
            jwt = new JWebToken(token);
            jwt.validate(getSecret);
            return jwt.getSubject();
        } catch (NoSuchAlgorithmException | JsonProcessingException e) {
            throw new RuntimeException("got exception while parsing token", e);
        }
    }

    public JWebToken(String sub, JsonNode aud, long expires, String secretKey)
            throws JsonMappingException, JsonProcessingException {
        this();
        payload = new ObjectNode(RESTObjectMapper.mapper().getNodeFactory());
        payload.put("sub", sub);
        payload.set("aud", aud);
        payload.put("exp", expires);
        payload.put("iat", LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
        payload.put("iss", ISSUER);
        payload.put("jti", UUID.randomUUID().toString()); // how do we use this?
        signature = hmacSha256(encodedHeader + "." + encode(payload), secretKey);
    }

    public JWebToken(String token) throws NoSuchAlgorithmException, JsonMappingException, JsonProcessingException {
        this();
        String[] parts = token.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid Token format");
        }
        if (encodedHeader.equals(parts[0])) {
            encodedHeader = parts[0];
        } else {
            throw new NoSuchAlgorithmException("JWT Header is Incorrect: " + parts[0]);
        }
        payload = (ObjectNode) RESTObjectMapper.mapper().readTree(decode(parts[1]));
        if (payload.isEmpty()) {
            throw new RuntimeException("Payload is Empty: ");
        }
        if (!payload.has("exp")) {
            throw new RuntimeException("Payload doesn't contain expiry " + payload);
        }
        signature = parts[2];
    }

    @Override
    public String toString() {
        return encodedHeader + "." + encode(payload) + "." + signature;
    }

    public void validate(Function<String, String> getSecret) {
        Long exp = payload.get("exp").asLong();
        String sub = payload.get("sub").asText();
        boolean cond1 = exp > (LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
        String generatedSignature = hmacSha256(encodedHeader + "." + encode(payload), getSecret.apply(sub));
        boolean cond2 = signature.equals(generatedSignature);
        if (!(cond1 && cond2)) {
            LOG.warn("validation is broken, exp=%s, sub=%s, cond1=%s, cond2=%s", exp, sub, cond1, cond2);
            throw new RuntimeException("validation is broken");
        }
    }

    public String getSubject() {
        return payload.get("sub").asText();
    }

    public List<String> getAudience() {
        ArrayNode arr = (ArrayNode) payload.get("aud");
        List<String> list = new ArrayList<>();
        for (int i = 0; i < arr.size(); i++) {
            list.add(arr.get(i).asText());
        }
        return list;
    }

    private static String encode(JsonNode obj) {
        return encode(obj.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static String encode(byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static String decode(String encodedString) {
        return new String(Base64.getUrlDecoder().decode(encodedString));
    }

    private String hmacSha256(String data, String secret) {
        try {

            // MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = secret.getBytes(StandardCharsets.UTF_8);// digest.digest(secret.getBytes(StandardCharsets.UTF_8));

            Mac sha256Hmac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKey = new SecretKeySpec(hash, "HmacSHA256");
            sha256Hmac.init(secretKey);

            byte[] signedBytes = sha256Hmac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return encode(signedBytes);
        } catch (NoSuchAlgorithmException | InvalidKeyException ex) {
            LOG.warn(ex.getMessage(), ex);
            return null;
        }
    }

}
