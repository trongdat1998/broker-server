package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder(builderClassName = "Builder")
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_withdraw_white_address")
public class WithdrawWhiteAddress {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private Long userId;
    private String tokenId;
    private String chainType;
    private String address;
    private String addressExt;
    private Integer status;
    private Long created;
    private Long updated;

}
