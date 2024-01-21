package io.bhex.broker.server.model;


import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@NoArgsConstructor
@AllArgsConstructor
@Builder(builderClassName = "Builder",toBuilder = true)
@Data
@Table(name = "tb_trade_competition_limit")
public class TradeCompetitionLimit {

    @Id
    private Long id;

    private Long orgId;

    private Long competitionId;

    private String competitionCode;

    private String enterIdStr;

    private Integer enterStatus;

    private String enterWhiteStr;

    private Integer enterWhiteStatus;

    private String limitStr;

    private Integer limitStatus;

    public List<Long> listEnterId(){

        if(StringUtils.isBlank(this.enterIdStr)){
            return Lists.newArrayList();
        }

        return Splitter.on(",")
                .omitEmptyStrings()
                .trimResults()
                .splitToList(this.enterIdStr)
                .stream()
                .map(s -> Long.parseLong(s)).collect(Collectors.toList());

    }

    public List<Long> listWhiteEnterId(){

        if(StringUtils.isBlank(this.enterWhiteStr)){
            return Lists.newArrayList();
        }

        return Splitter.on(",")
                .omitEmptyStrings()
                .trimResults()
                .splitToList(this.enterWhiteStr)
                .stream()
                .map(s -> Long.parseLong(s)).collect(Collectors.toList());

    }

    public void putEntryIds(Collection<Long> IdCollection) {
        this.enterIdStr= Joiner.on(",").join(IdCollection);
    }

    public void putWhiteEntryIds(Collection<Long> IdCollection) {
        this.enterWhiteStr=Joiner.on(",").join(IdCollection);
    }
}
