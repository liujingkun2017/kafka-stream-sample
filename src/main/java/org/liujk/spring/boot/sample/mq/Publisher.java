package org.liujk.spring.boot.sample.mq;

import cn.hutool.json.JSONUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Map;

@Component
public class Publisher {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    /**
     * 数据发送到kafka
     *
     * @param topic
     * @param topicKey
     * @param msgMap
     */
    public void sendTopicMsgToKafka(String topic, String topicKey, Map<String, String> msgMap) {

        ListenableFuture listenableFuture = kafkaTemplate.send(topic, topicKey, JSONUtil.toJsonStr(msgMap));
        listenableFuture.addCallback((object) -> {
            System.out.println("send success");
        }, (e) -> {
            System.out.println("send fail");
        });
    }

}
