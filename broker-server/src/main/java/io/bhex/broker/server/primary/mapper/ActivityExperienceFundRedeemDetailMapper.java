package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityExperienceFundRedeemDetail;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Mapper
public interface ActivityExperienceFundRedeemDetailMapper extends
        tk.mybatis.mapper.common.Mapper<ActivityExperienceFundRedeemDetail> {

    @Select("select * from tb_activity_experience_fund_redeem_detail where transfer_record_id = #{transferRecordId} and status < 2")
    List<ActivityExperienceFundRedeemDetail> getUnDoneDetails(@Param("transferRecordId") long transferRecordId);

}
