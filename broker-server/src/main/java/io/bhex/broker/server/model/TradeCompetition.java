package io.bhex.broker.server.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

@Builder(builderClassName = "Builder",toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
@Data
@Table(name = "tb_trade_competition")
public class TradeCompetition {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private String competitionCode;

    private String symbolId;

    private Date startTime;

    private Date endTime;

    private Integer type;

    // 0初始化 1正在比赛 2已经结束 3已无效
    private Integer status;

    private Integer effectiveQuantity;

    private String receiveTokenId;

    private String receiveTokenName;

    private BigDecimal receiveTokenAmount;

    private Date createTime;

    private Date updateTime;

    private String rankTypes;

    private Integer isReverse; //0 普通单个合约比赛 1正向组团比赛 2反向组团比赛

    public enum CompetitionStatus{
        UNKNOWN(-1),INIT(0),PROCESSING(1),FINISH(2),INVALID(3);
        private int value;
        private CompetitionStatus(int status){
            this.value=status;
        }

        public int getValue(){
            return this.value;
        }

        public static CompetitionStatus getEnum(int status){
            for(CompetitionStatus cs:CompetitionStatus.values()){
                if(cs.getValue()==status){
                    return cs;
                }
            }

            return UNKNOWN;
        }
    }
}
