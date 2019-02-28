package com.data.config;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PulasrClientConfig {

    @Bean
    public PulsarClient pulsarClient() {
        PulsarClient client = null;
        try {
            client = PulsarClient.builder()
                    .serviceUrl("pulsar://192.168.25.56:6650")
                    .build();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

        return client;

    }
}
