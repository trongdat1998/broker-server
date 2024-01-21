package io.bhex.broker.server.primary.mapper;

/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.mapper
 *@Date 2018/8/23
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

import io.bhex.broker.server.model.WithdrawAddress;

@Mapper
public interface WithdrawAddressMapper {

    String COLUMNS = "id, org_id, user_id, token_id, chain_type, token_name, address, address_ext, remark, created";

    @Insert("INSERT INTO tb_withdraw_address(org_id, user_id, token_id, chain_type, token_name, address, address_ext, remark, created) "
            + "VALUES(#{orgId}, #{userId}, #{tokenId}, #{chainType}, #{tokenName}, #{address}, #{addressExt}, #{remark}, #{created})")
    @Options(useGeneratedKeys = true, keyColumn = "id", keyProperty = "id")
    int insert(WithdrawAddress withdrawAddress);

    @Select("SELECT " + COLUMNS + " FROM tb_withdraw_address where user_id = #{userId} and `status` = 0 ")
    List<WithdrawAddress> queryByUserId(@Param("userId") Long userId);

    @Select("SELECT " + COLUMNS + " FROM tb_withdraw_address where id = #{id} AND user_id = #{userId}")
    WithdrawAddress getById(@Param("id") Long id, @Param("userId") Long userId);

    @Select({"<script>"
            , "SELECT " + COLUMNS + " FROM tb_withdraw_address "
            , "WHERE user_id = #{userId} AND token_id = #{tokenId} "
            , "<if test=\"chainType != null and chainType != ''\">AND chain_type=#{chainType}</if> "
            , "AND `status` = 0 "
            , "</script>"
    })
    List<WithdrawAddress> queryByTokenId(@Param("userId") Long userId, @Param("tokenId") String tokenId, @Param("chainType") String chainType);

    @Select("SELECT " + COLUMNS + " FROM tb_withdraw_address WHERE user_id = #{userId} AND token_id = 'USDT' and (chain_type ='' or chain_type='OMNI') and `status`=0")
    List<WithdrawAddress> queryOMNIAddress(@Param("userId") Long userId);

    @Update("UPDATE tb_withdraw_address SET `status` = #{status} where id = #{id} AND user_id = #{userId} ")
    int delete(@Param("id") Long id, @Param("userId") Long userId, @Param("status") int status);

    @Select({"<script>"
            , "SELECT " + COLUMNS + " FROM tb_withdraw_address "
            , "WHERE user_id = #{userId} AND token_id = #{tokenId} "
            , "AND address = #{address} "
            , "<if test=\"addressExt != null and addressExt != ''\">AND address_ext=#{addressExt}</if> "
            , "AND `status`= 0 "
            , "</script>"
    })
    List<WithdrawAddress> getByAddress(@Param("userId") Long userId, @Param("address") String address, @Param("addressExt") String addressExt, @Param("tokenId") String tokenId);

    @Update("update tb_withdraw_address set request_num = request_num + 1 where user_id = #{userId} and token_id = #{tokenId} and address = #{address} AND `status`= 0")
    int updateRequestNum(@Param("userId") Long userId, @Param("tokenId") String tokenId, @Param("address") String address);

    //临时需求
    @Update("update tb_withdraw_address set request_num = 1,success_num = 1 where org_id = 6004 and user_id = 423782248459124224 and request_num = 0 and success_num = 0")
    int updateSuccessNum();
}

