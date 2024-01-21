package io.bhex.broker.server.domain;

import lombok.AllArgsConstructor;

/**
 * @Description: 对应 tb_user 中user_status
 * @Date: 2018/8/23 下午7:28
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */

@AllArgsConstructor
public enum UserStatusEnum {

    ENABLE(1, "启用"),
    DISABLED(2, "禁用");

    private int value;
    private String description;


    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
