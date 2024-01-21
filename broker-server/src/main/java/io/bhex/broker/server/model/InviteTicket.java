package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_invite_ticket")
public class InviteTicket {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long actId;

    private Long userId;

    private Long accountId;

    private String tokenId;

    private Double amount;

    private Integer status;

    private Timestamp overdueAt;

    private Timestamp createdAt;

    private Timestamp updatedAt;

    public enum Status {

        WAITING(0, "等候完成中"),
        FINISH(1, "已完成"),
        OVERDUE(2, "已过期");

        Status(int status, String desc) {
            this.status = status;
            this.desc = desc;
        }

        private int status;

        private String desc;

        public int getStatus() {
            return status;
        }

        public String getDesc() {
            return desc;
        }

        public static Status valuesOf(int status) {
            for (Status st : Status.values()) {
                if (st.getStatus() == status) {
                    return st;
                }
            }
            return null;
        }
    }


}
