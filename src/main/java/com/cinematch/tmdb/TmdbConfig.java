package com.cinematch.tmdb;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

@Configuration
public class TmdbConfig {

    @Bean
    public RestTemplate tmdbRestTemplate(@Value("${tmdb.token:}") String token) {
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.getInterceptors().add((request, body, execution) -> {
            if (token != null && !token.isBlank()) {
                request.getHeaders().set(HttpHeaders.AUTHORIZATION, "Bearer " + token);
            }
            request.getHeaders().setAccept(List.of(MediaType.APPLICATION_JSON));
            ClientHttpResponse response = execution.execute(request, body);
            String contentEncoding = response.getHeaders().getFirst(HttpHeaders.CONTENT_ENCODING);
            if (contentEncoding != null && contentEncoding.toLowerCase().contains("gzip")) {
                return new GzipDecompressingResponse(response);
            }
            return response;
        });
        return restTemplate;
    }

    private static class GzipDecompressingResponse implements ClientHttpResponse {
        private final ClientHttpResponse delegate;
        private InputStream decompressed;

        GzipDecompressingResponse(ClientHttpResponse delegate) {
            this.delegate = delegate;
        }

        @Override
        public InputStream getBody() throws IOException {
            if (decompressed == null) {
                decompressed = new GZIPInputStream(delegate.getBody());
            }
            return decompressed;
        }

        @Override
        public HttpHeaders getHeaders() {
            HttpHeaders headers = new HttpHeaders();
            headers.putAll(delegate.getHeaders());
            headers.remove(HttpHeaders.CONTENT_ENCODING);
            headers.remove(HttpHeaders.CONTENT_LENGTH);
            return headers;
        }

        @Override
        public HttpStatusCode getStatusCode() throws IOException {
            return delegate.getStatusCode();
        }

        @Override
        public String getStatusText() throws IOException {
            return delegate.getStatusText();
        }

        @Override
        public void close() {
            if (decompressed != null) {
                try { decompressed.close(); } catch (IOException ignored) {}
            }
            delegate.close();
        }
    }
}
