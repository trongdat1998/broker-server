package io.bhex.broker.server.grpc.server.service;

import io.bhex.broker.grpc.admin.GetWithdrawAddressResponse;
import io.bhex.broker.grpc.admin.UserWithdrawAddress;
import io.bhex.broker.server.primary.mapper.WithdrawAddressMapper;
import io.bhex.broker.server.model.WithdrawAddress;
import io.bhex.broker.server.util.BeanCopyUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Date: 2018/8/23 下午5:22
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */

@Slf4j
@Service("withdrawAddressService")
public class WithdrawAddressService {

    @Resource
    private WithdrawAddressMapper withdrawAddressMapper;

    public GetWithdrawAddressResponse getWithdrawAddress(Long userId) {

        List<WithdrawAddress> withdrawAddresses = withdrawAddressMapper.queryByUserId(userId);
        if (CollectionUtils.isEmpty(withdrawAddresses)) {
            return GetWithdrawAddressResponse.newBuilder().build();
        }

        List<UserWithdrawAddress> addressesResponse = withdrawAddresses.stream().map(withdrawAddress -> {
            UserWithdrawAddress.Builder builder = UserWithdrawAddress.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(withdrawAddress, builder);
            return builder.build();
        }).collect(Collectors.toList());

        return GetWithdrawAddressResponse.newBuilder().addAllUserWithdrawAddresses(addressesResponse).build();
    }
}
