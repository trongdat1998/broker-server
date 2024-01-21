package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Table;
import java.sql.Timestamp;

/**
 * @author zhaojiankun
 * @date 2018/09/20
 */
@Table(name = "tb_fcode_apply")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FcodeApply {

    private Long id;

    private Long brokerId;

    private String imgUrl;

    private String email;

    private String nationalCode;

    private String phoneNo;

    private Integer approveStatus;

    private Timestamp createdAt;

    private Timestamp updatedAt;

    public class ApproveStatus {

        public static final int NO_APPROVE = 0;

        public static final int PASS = 1;

        public static final int REJECT = 2;

    }

}
