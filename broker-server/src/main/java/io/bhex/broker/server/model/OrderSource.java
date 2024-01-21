package io.bhex.broker.server.model;

import lombok.Builder;
import lombok.Data;

import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@Table(name = "tb_order_source")
public class OrderSource {

    public static final Integer BINARY_ORDER_SOURCE_BITS = 10;
    public static final String DEFAULT_BINARY_ORDER_SOURCE = "0000000000";
    private static final String[] ORDER_SOURCE_LEFT_PADDING0 = new String[]{"", "0", "00", "000", "0000", "00000", "000000", "0000000", "00000000", "000000000"};

    @Id
    private Long id;
    private Long orgId;
    private String source; //
    private String sourceName;
    private Integer sourceId; // auto increase from 1 to 1024, with each broker
    //    private Integer binarySourceIdBits; // binary sourceId bits
    private String binarySource; // binary sourceId with left padding 0, length is {{ binarySourceBits }}
    private Integer status;
    private Long created;
    private Long updated;

    public String transferBinarySource() {
        String binarySource = Integer.toBinaryString(this.sourceId);
        if (binarySource.length() < BINARY_ORDER_SOURCE_BITS) {
            binarySource = ORDER_SOURCE_LEFT_PADDING0[BINARY_ORDER_SOURCE_BITS - binarySource.length()] + binarySource;
        }
        return binarySource;
    }

}
