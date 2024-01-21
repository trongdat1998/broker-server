package io.bhex.broker.server.domain;

import com.google.common.base.Splitter;
import io.bhex.broker.server.model.TradeCompetitionExt;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.Transient;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TradeCompetitionInfo {

    /**
     * config info
     **/
    private Long id;

    private Long orgId;

    private String competitionCode;

    private String symbolId;

    private Date startTime;

    private Date endTime;

    private Integer type;

    private Integer status;

    private String receiveTokenId;

    private String receiveTokenName;

    private BigDecimal receiveTokenAmount;

    private Integer effectiveQuantity;

    /**
     * common info
     **/

    private Map<String, TradeCompetitionExt> extMap;


    /**
     * limit info
     **/

    private String enterIdStr;

    private Integer enterStatus;

    private String enterWhiteStr;

    private Integer enterWhiteStatus;

    private String limitStr;

    private Integer limitStatus;

    private String rankTypes;

    private Integer isReverse; //1正向 2反向


    @Transient
    public List<Long> getEnterIdList() {
        if (StringUtils.isEmpty(this.enterIdStr)) {
            return new ArrayList<>();
        }

        return Splitter.on(",")
                .omitEmptyStrings()
                .trimResults()
                .splitToList(this.enterIdStr)
                .stream()
                .map(s -> Long.parseLong(s)).collect(Collectors.toList());
    }


    @Transient
    public List<Long> getEnterIdWhiteList() {
        if (StringUtils.isEmpty(this.enterWhiteStr)) {
            return new ArrayList<>();
        }

        return Splitter.on(",")
                .omitEmptyStrings()
                .trimResults()
                .splitToList(this.enterWhiteStr)
                .stream()
                .map(s -> Long.parseLong(s)).collect(Collectors.toList());
    }

    @Transient
    public List<Integer> getRankTypeList() {
        if (StringUtils.isEmpty(this.rankTypes)) {
            return new ArrayList<>();
        }

        return Splitter.on(",")
                .omitEmptyStrings()
                .trimResults()
                .splitToList(this.rankTypes)
                .stream()
                .map(s -> Integer.parseInt(s)).collect(Collectors.toList());
    }

    public TradeCompetitionExt getExtByLanguage(String language) {
        return this.extMap.get(language);
    }
}
