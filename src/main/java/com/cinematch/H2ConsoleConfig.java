package com.cinematch;

import java.util.LinkedHashMap;
import java.util.Map;

import org.h2.server.web.JakartaWebServlet;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class H2ConsoleConfig {

    @Bean
    @ConditionalOnProperty(prefix = "spring.h2.console", name = "enabled", havingValue = "true")
    ServletRegistrationBean<JakartaWebServlet> h2ConsoleServlet() {
        ServletRegistrationBean<JakartaWebServlet> registrationBean =
                new ServletRegistrationBean<>(new JakartaWebServlet(), "/h2-console/*");

        Map<String, String> initParameters = new LinkedHashMap<>();
        initParameters.put("webAllowOthers", "true");
        initParameters.put("trace", "false");
        registrationBean.setInitParameters(initParameters);
        registrationBean.setLoadOnStartup(1);
        return registrationBean;
    }
}
