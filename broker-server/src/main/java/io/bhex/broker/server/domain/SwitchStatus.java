package io.bhex.broker.server.domain;

import lombok.Builder;
import lombok.Data;

/**
 * @Description:
 * @Date: 2019/10/25 下午2:26
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Data
@Builder
public class SwitchStatus {
    private boolean existed; //开关是否存在
    private boolean open;    // true-on false-off
}