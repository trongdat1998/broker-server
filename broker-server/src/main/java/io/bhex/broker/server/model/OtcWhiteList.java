package io.bhex.broker.server.model;

import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * otc白名单
 *
 * @author lizhen
 * @date 2018-11-13
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_white_list")
public class OtcWhiteList {
    /**
     * id
     */
    @Id
    private Long id;

    private Long orgId;

    private Long userId;
}
