package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_red_packet_theme")
public class RedPacketTheme {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private String themeId;
    private String remark; // 主题说明
    private String themeContent; // [{"language":"zh_CN","backgroundUrl":"https://xxx","backgroundColor":"#000000","slogan":"恭喜发财"},{}]
    //    private transient List<Theme> themeList;
    private Integer status;
    private Integer customOrder;
    private Integer position; // RED_PACKET_INDEX 2 RED_PACKET_INDEX
    private Long created;
    private Long updated;


    public io.bhex.broker.grpc.red_packet.RedPacketTheme convertGrpcObj() {
        return io.bhex.broker.grpc.red_packet.RedPacketTheme.newBuilder()
                .setId(this.id)
                .setOrgId(this.orgId)
                .setThemeId(this.getThemeId())
                .setTheme(this.themeContent)
                .setStatus(this.status)
                .setCustomIndex(this.customOrder)
                .setPosition(this.position)
                .build();
    }

}
