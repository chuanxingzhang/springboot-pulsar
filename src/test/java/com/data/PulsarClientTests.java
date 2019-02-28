package com.data;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class PulsarClientTests {

    @Resource
    private PulsarClient pulsarClient;


    @Test
    @Ignore
    public void pulsarProducerByte() {
        Producer<byte[]> producer = null;
        try {
            producer = pulsarClient.newProducer()
                    .topic("my-topic")
                    .create();
            int i = 1;
            producer.sendAsync(("My message1" + i).getBytes());

        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * String 类型 可以支持很多类型Schema.
     */
    @Test
    public void pulsarProducerString() {
        Producer<String> producer = null;
        try {

            producer = pulsarClient.newProducer(Schema.STRING).topic("persistent://data/data/my-topic").producerName("produce")
                    .create();
            int i = 0;
            while (true) {
                MessageId messageId = producer.send("hello String" + i++);
                log.info("Message with ID {} successfully sent", messageId);

                Thread.sleep(100);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
