package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.TokenOption;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.List;

@org.apache.ibatis.annotations.Mapper
@Component(value = "tokenOptionMapper")
public interface TokenOptionMapper extends tk.mybatis.mapper.common.Mapper<TokenOption> {

    @Select("select * from tb_token_option where settlement_date<=#{settlementDate} order by settlement_date desc limit #{start}, #{offset}")
    List<TokenOption> selectTokenOptions(@Param("settlementDate") Timestamp settlementDate, @Param("start") int start, @Param("offset") int offset);

    @Select("select count(1) from tb_token_option where settlement_date<=#{settlementDate}")
    int selectTokenOptionCount(Timestamp now);

    @Select("select id from tb_token_option where token_id = #{tokenId}")
    Long selectIdFromTokenOptionByTokenId(String tokenId);
}
