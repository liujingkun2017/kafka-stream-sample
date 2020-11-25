package org.liujk.spring.boot.sample.controller;

import org.liujk.spring.boot.sample.common.Constants;
import org.liujk.spring.boot.sample.mq.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 测试druid的sample
 */
@RestController("/druid")
public class DruidSampleController {

    @Autowired
    private Publisher publisher;

    @RequestMapping("/druid-publish")
    public String druidPublish() {

        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                publishMesToDruid();
            }).start();
        }

        return "ok";
    }


    private void publishMesToDruid() {
        int i = 0;
        while (i < 10000000) {

            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("thread=" + Thread.currentThread().getName() + "i=" + i);

            Map<String, Object> data = new HashMap<>();

            data.put("timestamp", String.valueOf(new Date().getTime()));

            if (i % 3 == 0) {
                data.put("name", "zhangsan" + i);
                data.put("gender", "male");
                data.put("city", "chongqing");
                data.put("age", "30");
                data.put("height", "185");
                data.put("weight", "80");
                data.put("income", "1000");
                data.put("expend", "500");
            }
            if (i % 3 == 1) {
                data.put("name", "zhangsan" + i);
                data.put("gender", "female");
                data.put("city", "shanghai");
                data.put("age", "28");
                data.put("height", "165");
                data.put("weight", "50");
                data.put("income", "2000");
                data.put("expend", "1000");
            }
            if (i % 3 == 2) {
                data.put("name", "zhangsan" + i);
                data.put("gender", "female");
                data.put("city", "beijing");
                data.put("age", "30");
                data.put("height", "185");
                data.put("weight", "80");
                data.put("income", "1900");
                data.put("expend", "850");
            }

            String topic = Constants.KAFKA_DRUID_TOPIC + i % 15;
            publisher.sendTopicMsgToKafka(topic, String.valueOf(data.get("name")), data);

            ++i;
        }

        System.out.println("finished");
    }
}
