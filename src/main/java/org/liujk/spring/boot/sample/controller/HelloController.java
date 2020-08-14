package org.liujk.spring.boot.sample.controller;

import cn.hutool.json.JSONUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.liujk.spring.boot.sample.common.Constants;
import org.liujk.spring.boot.sample.dto.TxSampleDTO;
import org.liujk.spring.boot.sample.dto.TxSampleStreamDTO;
import org.liujk.spring.boot.sample.mq.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.*;

@RestController
public class HelloController {

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServers;

    @Autowired
    private Publisher publisher;

    @RequestMapping("/hello-world")
    public String index() {
        return "hello world!";
    }

    @RequestMapping("/publish")
    public String publish() {

//        KafkaStreams kafkaStreams = start();

        for (int i = 0; i < 20; i++) {
            TxSampleDTO txSampleDTO = new TxSampleDTO();
            txSampleDTO.setTimestamp(new Date().getTime());
            txSampleDTO.setId(i);
            txSampleDTO.setName("sample-app");
//            txSampleDTO.setDuration(String.valueOf(new Random().nextInt(20)));
            Map<String, Object> msgMap = new HashMap<>();
            msgMap.put("timestamp", txSampleDTO.getTimestamp());
            msgMap.put("name", txSampleDTO.getName());
            msgMap.put("id", txSampleDTO.getId());
//            msgMap.put("duration", txSampleDTO.getDuration());
            publisher.sendTopicMsgToKafka(Constants.KAFKA_SOURCE_TOPIC, txSampleDTO.getName(), msgMap);
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

//        stop(kafkaStreams);

        return "ok!";
    }


    @RequestMapping("/publish-one")
    public String publishOne() {

        try {
            Thread.sleep(40 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 10000; i < 10020; i++) {
            TxSampleDTO txSampleDTO = new TxSampleDTO();
            txSampleDTO.setTimestamp(new Date().getTime());
            txSampleDTO.setName("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
            txSampleDTO.setId(i);
//        txSampleDTO.setDuration(String.valueOf(new Random().nextInt(20)));
            Map<String, Object> msgMap = new HashMap<>();
            msgMap.put("timestamp", txSampleDTO.getTimestamp());
            msgMap.put("name", txSampleDTO.getName());
            msgMap.put("id", txSampleDTO.getId());
            publisher.sendTopicMsgToKafka(Constants.KAFKA_SOURCE_TOPIC, txSampleDTO.getName(), msgMap);
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return "ok!";
    }


    private KafkaStreams start() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Constants.APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KGroupedStream stream = builder.<String, String>stream(Constants.KAFKA_SOURCE_TOPIC).groupByKey();

        KTable aggregate = stream
                .windowedBy(TimeWindows.of(Duration.ofSeconds(1)).advanceBy(Duration.ofSeconds(1)))
                .aggregate(
                        () -> null,
                        (aggKey, newValue, aggValue) -> {

                            if (StringUtils.isEmpty(aggValue)) {
                                TxSampleStreamDTO txSampleStreamDTO = JSONUtil.toBean((String) newValue, TxSampleStreamDTO.class);
                                txSampleStreamDTO.setCount(1);
                                return JSONUtil.toJsonStr(txSampleStreamDTO);
                            }

                            TxSampleStreamDTO txSampleStreamDTO = JSONUtil.toBean((String) aggValue, TxSampleStreamDTO.class);
                            txSampleStreamDTO.setCount(txSampleStreamDTO.getCount() + 1);
                            return JSONUtil.toJsonStr(txSampleStreamDTO);
                        },
                        Materialized.<String, String, KeyValueStore<String, String>>as(Constants.APPLICATION_ID + "-action")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                );
        aggregate.toStream().to(Constants.KAFKA_TARGET_TOPIC);
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        return streams;
    }


    private void stop(KafkaStreams streams) {
        streams.close();
    }

}
