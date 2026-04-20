package movie.web.login.tmdb;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import java.util.List;

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
            return execution.execute(request, body);
        });
        return restTemplate;
    }
}
