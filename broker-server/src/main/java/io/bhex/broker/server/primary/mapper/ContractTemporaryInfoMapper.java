package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

import io.bhex.broker.server.model.ContractTemporaryInfo;
import tk.mybatis.mapper.common.Mapper;

@org.apache.ibatis.annotations.Mapper
public interface ContractTemporaryInfoMapper extends Mapper<ContractTemporaryInfo> {

    @Select("select *from tb_contract_temporary_info where status < 2")
    List<ContractTemporaryInfo> queryAllContractTemporaryInfo();


    @Select("select *from tb_contract_temporary_info where status = 0")
    List<ContractTemporaryInfo> queryAllInitContractTemporaryInfo();

    @Update("update tb_contract_temporary_info set lock_order_id = #{lockOrderId},unlock_order_id = #{unlockOrderId} where id = #{id}")
    int updateLockOrderIdById(@Param("lockOrderId") Long lockOrderId, @Param("unlockOrderId") Long unlockOrderId, @Param("id") Long id);

    @Update("update tb_contract_temporary_info set status = #{status} where id = #{id}")
    int updateStatusById(@Param("status") Integer status, @Param("id") Long id);
}
