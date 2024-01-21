package io.bhex.broker.server.model;

import lombok.Builder;
import lombok.Data;

import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@Table(name = "tb_find_pwd_record")
public class FindPwdRecord {

    @Id
    private Long id;
    private String requestId; // status = -1 的 情况下，requestId不会被再次验证
    private Long orgId;
    private Long userId;
    private Integer findType; // 手机 or 邮箱
    private Integer isOldVersionRequest; // 是否旧版本请求，独立部署的前端，不需要2fa
    private Integer need2fa; // 是否需要2fa
    private Integer authType; // 2fa方式 0 不需要
    private Long authOrderId; // 2fa邮箱或者手机号请求的orderId
    private Integer authRequestCount; // 2fa请求次数
    private Integer hasBalance; // 是否有资产
    private String balanceInfo; // 资产信息
    private Integer frozenLogin; // 找回密码后是否冻结登录
    private String ip; // ip
    private Integer status; // -1:验证码错误; -2:等待2fa; -3:更换找回方式; -4:2fa Failed; 0: skip 2fa 1: 成功
    private String platform;
    private String userAgent;
    private String language;
    private String appBaseHeader;
    private Long created;
    private Long updated;

}
