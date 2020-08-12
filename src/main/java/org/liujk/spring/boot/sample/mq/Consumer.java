package org.liujk.spring.boot.sample.mq;

import org.liujk.spring.boot.sample.common.Constants;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Component
public class Consumer {

    @KafkaListener(topics = Constants.KAFKA_TOPIC)
    public void consume(byte[] dataBtyes) throws Exception {

        String dataStr = new String(dataBtyes, "UTF-8");
        System.out.println(dataStr);
    }

}
