package org.liujk.spring.boot.sample;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.liujk.spring.boot.sample.common.Constants;
import org.liujk.spring.boot.sample.mq.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DruidSampleTest {

    @Autowired
    private Publisher publisher;

    @Test
    public void druidPublishTest() {

        int i = 0;
        while (i < 100000) {

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

            publisher.sendTopicMsgToKafka(Constants.KAFKA_DRUID_TOPIC, String.valueOf(data.get("name")), data);

            ++i;
        }

        System.out.println("finished");

    }

}
