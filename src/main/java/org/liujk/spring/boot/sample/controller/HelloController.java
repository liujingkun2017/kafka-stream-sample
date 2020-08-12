package org.liujk.spring.boot.sample.controller;

import org.liujk.spring.boot.sample.common.Constants;
import org.liujk.spring.boot.sample.dto.TxSampleDTO;
import org.liujk.spring.boot.sample.mq.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class HelloController {

    @Autowired
    private Publisher publisher;

    @RequestMapping("/hello-world")
    public String index() {
        return "hello world!";
    }

    @RequestMapping("/publish")
    public String publish() {

        TxSampleDTO txSampleDTO = new TxSampleDTO();
        txSampleDTO.setName("sample-app");
        txSampleDTO.setDuration("xx");
        Map<String, String> msgMap = new HashMap<>();
        msgMap.put("name", txSampleDTO.getName());
        msgMap.put("duration111", txSampleDTO.getDuration());
        publisher.sendTopicMsgToKafka(Constants.KAFKA_TOPIC, txSampleDTO.getName(), msgMap);

        return "ok!";
    }

}
