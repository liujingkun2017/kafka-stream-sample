package org.liujk.spring.boot.sample.dto;

import lombok.Data;

import java.util.Map;

@Data
public class TxSampleStreamDTO {

    private String name;

//    private String duration;

    private int count;

    private Map<Long, Long> datas;

}
