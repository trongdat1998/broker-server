package io.bhex.broker.server.model;

import java.util.Date;

import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_white_list_limit")
public class OtcWhiteListLimit {
    /**
     * id
     */
    @Id
    private Long id;

    private Long orgId;

    private Integer status;

    private Date created;
}
