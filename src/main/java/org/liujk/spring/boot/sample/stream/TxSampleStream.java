//package org.liujk.spring.boot.sample.stream;
//
//import cn.hutool.json.JSONUtil;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.streams.KafkaStreams;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.StreamsConfig;
//import org.apache.kafka.streams.Topology;
//import org.apache.kafka.streams.kstream.KGroupedStream;
//import org.apache.kafka.streams.kstream.KTable;
//import org.apache.kafka.streams.kstream.Materialized;
//import org.apache.kafka.streams.kstream.TimeWindows;
//import org.apache.kafka.streams.state.KeyValueStore;
//import org.liujk.spring.boot.sample.common.Constants;
//import org.liujk.spring.boot.sample.dto.TxSampleStreamDTO;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.stereotype.Component;
//import org.springframework.util.StringUtils;
//
//import javax.annotation.PostConstruct;
//import java.time.Duration;
//import java.util.Properties;
//
//@Component
//public class TxSampleStream {
//
//    @Value("${spring.kafka.bootstrap-servers}")
//    private String kafkaServers;
//
//    @PostConstruct
//    public void init() {
//
//        Properties props = new Properties();
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Constants.APPLICATION_ID);
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//
//        StreamsBuilder builder = new StreamsBuilder();
//
//        KGroupedStream stream = builder.<String, String>stream(Constants.KAFKA_SOURCE_TOPIC).groupByKey();
//
//        KTable aggregate = stream
//                .windowedBy(TimeWindows.of(Duration.ofSeconds(1)).advanceBy(Duration.ofSeconds(1)))
//                .aggregate(
//                        () -> null,
//                        (aggKey, newValue, aggValue) -> {
//
//                            if (StringUtils.isEmpty(aggValue)) {
//                                TxSampleStreamDTO txSampleStreamDTO = JSONUtil.toBean((String) newValue, TxSampleStreamDTO.class);
//                                txSampleStreamDTO.setCount(1);
//                                return JSONUtil.toJsonStr(txSampleStreamDTO);
//                            }
//
//                            TxSampleStreamDTO txSampleStreamDTO = JSONUtil.toBean((String) aggValue, TxSampleStreamDTO.class);
//                            txSampleStreamDTO.setCount(txSampleStreamDTO.getCount() + 1);
//                            return JSONUtil.toJsonStr(txSampleStreamDTO);
//                        },
//                        Materialized.<String, String, KeyValueStore<String, String>>as(Constants.APPLICATION_ID + "-action")
//                                .withKeySerde(Serdes.String())
//                                .withValueSerde(Serdes.String())
//                );
//
//
//        aggregate.toStream().to(Constants.KAFKA_TARGET_TOPIC);
//
//        Topology topology = builder.build();
//        KafkaStreams streams = new KafkaStreams(topology, props);
//
//        streams.start();
//
//
//    }
//
//
//}
