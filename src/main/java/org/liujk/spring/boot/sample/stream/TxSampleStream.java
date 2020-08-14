package org.liujk.spring.boot.sample.stream;

import cn.hutool.json.JSONUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.liujk.spring.boot.sample.common.Constants;
import org.liujk.spring.boot.sample.dto.TxSampleDTO;
import org.liujk.spring.boot.sample.dto.TxSampleStreamDTO;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Component
public class TxSampleStream {

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServers;

    @PostConstruct
    public void init() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Constants.APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractor.class);

        StreamsBuilder builder = new StreamsBuilder();

        KGroupedStream stream = builder.<String, String>stream(Constants.KAFKA_SOURCE_TOPIC).groupByKey();

        KTable aggregate = stream
                .windowedBy(TimeWindows.of(Duration.ofSeconds(1))
                        .advanceBy(Duration.ofSeconds(1))
                        .grace(Duration.ofMillis(1 * 60 * 1000))
                )
                .aggregate(
                        () -> null,
                        (aggKey, newValue, aggValue) -> {
                            TxSampleDTO txSampleDTO = JSONUtil.toBean((String) newValue, TxSampleDTO.class);

                            if (StringUtils.isEmpty(aggValue)) {
                                TxSampleStreamDTO txSampleStreamDTO = new TxSampleStreamDTO();
                                txSampleStreamDTO.setCount(1);
                                txSampleStreamDTO.setName(txSampleDTO.getName());
                                Map<Long, Long> datas = new HashMap<>();
                                datas.put(txSampleDTO.getId(), txSampleDTO.getTimestamp());
                                txSampleStreamDTO.setDatas(datas);
                                return JSONUtil.toJsonStr(txSampleStreamDTO);
                            }

                            TxSampleStreamDTO txSampleStreamDTO = JSONUtil.toBean((String) aggValue, TxSampleStreamDTO.class);
                            txSampleStreamDTO.setCount(txSampleStreamDTO.getCount() + 1);
                            Map<Long, Long> datas = txSampleStreamDTO.getDatas();
                            datas.put(txSampleDTO.getId(), txSampleDTO.getTimestamp());
                            txSampleStreamDTO.setDatas(datas);
                            return JSONUtil.toJsonStr(txSampleStreamDTO);
                        },
                        Materialized.<String, String, KeyValueStore<String, String>>as(Constants.APPLICATION_ID + "-action")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));


        aggregate.toStream().to(Constants.KAFKA_TARGET_TOPIC);

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.start();


    }


}
