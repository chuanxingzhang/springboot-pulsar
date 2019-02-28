package com.data.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Configuration
@Slf4j
public class PulsarConsumerConfig {
    @Resource
    private PulsarClient pulsarClient;


    @PostConstruct
    public void pulsarConsumer() {
        new Thread(() -> {
            log.info("消费者启动");
            Consumer consumer = null;
            try {

                consumer = pulsarClient.newConsumer()
                        .topic("persistent://data/data/my-topic")
                        .subscriptionName("my-subscription")
//                        .subscriptionType(SubscriptionType.Failover)  //可以设置消费者类型,默认为独享
                        .subscribe();
                while (!Thread.currentThread().isInterrupted()) {
                    Message msg = consumer.receive();

                    log.info("Message received: {},{}", msg.getMessageId(), new String(msg.getData()));

                    consumer.acknowledge(msg);
                }

            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        }).start();

    }
}
