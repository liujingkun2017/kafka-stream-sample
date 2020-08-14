package org.liujk.spring.boot.sample.mq;

import org.liujk.spring.boot.sample.common.Constants;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Date;


@Component
public class Consumer {

    public static int count = 0;

    @KafkaListener(topics = Constants.KAFKA_TARGET_TOPIC)
    public void consume(byte[] dataBtyes) throws Exception {

        String dataStr = new String(dataBtyes, "UTF-8");
        System.out.println("Consumer：" + dataStr + ", time:" + new Date().getTime());

        count++;
        System.out.println("count:" + count);
        System.out.println("----------------------------------------");
    }

}
