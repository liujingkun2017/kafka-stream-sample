package org.liujk.spring.boot.sample.stream;

import cn.hutool.json.JSONUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.liujk.spring.boot.sample.dto.TxSampleDTO;

public class TransactionTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        TxSampleDTO txSampleDTO = JSONUtil.toBean((String) consumerRecord.value(), TxSampleDTO.class);
        return txSampleDTO.getTimestamp();
    }
}
