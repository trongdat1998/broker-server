/*
 ************************************
 * @项目名称: broker
 * @文件名称: User
 * @Date 2018/05/22
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.server.model;

import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * User Entity
 *
 * @author will.zhao@bhex.io
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_user")
public class User {

    @Id
    private Long id;

    @Column(name = "org_id")
    private Long orgId;

    @Column(name = "user_id")
    private Long userId;

    @Column(name = "bh_user_id")
    private Long bhUserId;

    @Column(name = "national_code")
    private String nationalCode;

    @Column(name = "mobile")
    private String mobile;

    @Column(name = "concat_mobile")
    private String concatMobile; // (national_code + mobile)

    @Column(name = "email")
    private String email;

    @Column(name = "email_alias")
    private String emailAlias;

    @Column(name = "register_type")
    private Integer registerType;

    @Column(name = "nickname")
    private String nickname;

    @Column(name = "avatar")
    private String avatar;

    @Column(name = "ip")
    private String ip;

    @Column(name = "user_status")
    private Integer userStatus;

    @Column(name = "input_invite_code")
    private String inputInviteCode;

    @Column(name = "invite_user_id")
    private Long inviteUserId;

    @Column(name = "second_level_invite_user_id")
    private Long secondLevelInviteUserId;

    @Column(name = "invite_code")
    private String inviteCode;

    @Column(name = "bind_ga")
    private Integer bindGA;

    @Column(name = "bind_trade_pwd")
    private Integer bindTradePwd;

    @Column(name = "bind_password")
    private Integer bindPassword;

    @Column(name = "auth_type")
    private Integer authType;

    @Column(name = "anti_phishing_code")
    private String antiPhishingCode;

    @Column(name = "user_type")
    private Integer userType;

    @Column(name = "channel")
    private String channel;

    @Column(name = "source")
    private String source;

    @Column(name = "platform")
    private String platform;

    @Column(name = "user_agent")
    private String userAgent;

    @Column(name = "language")
    private String language;

    @Column(name = "app_base_header")
    private String appBaseHeader;

    @Column(name = "created")
    private Long created;

    @Column(name = "updated")
    private Long updated;

    @Column(name = "is_vip")
    private Integer isVip;

    @Column(name = "is_virtual")
    private Integer isVirtual;

    @Column(name = "custom_label_ids")
    private String customLabelIds;

    public List<Long> getCustomLabelIdList() {
        if (StringUtils.isEmpty(customLabelIds)) {
            return new ArrayList<>();
        }
        List<Long> result = new ArrayList<>();
        String[] labelIdsStrArr = customLabelIds.split(",");
        for (String labelIdStr : labelIdsStrArr) {
            if (StringUtils.isNotEmpty(labelIdStr)) {
                result.add(Long.parseLong(labelIdStr));
            }
        }
        return result;
    }

    public void setCustomLabelIdList(List<Long> customLabelIdList) {
        customLabelIds = String.join(",", customLabelIdList.stream().map(id -> id.toString()).collect(Collectors.toList()));
    }

    public void setNationalCode(String nationalCode) {
        this.nationalCode = Strings.nullToEmpty(nationalCode);
    }

    public void setMobile(String mobile) {
        this.mobile = Strings.nullToEmpty(mobile);
    }

    public void setEmail(String email) {
        this.email = Strings.nullToEmpty(email);
    }

    public void setEmailAlias(String emailAlias) {
        this.emailAlias = Strings.nullToEmpty(emailAlias);
    }

    public void setAvatar(String avatar) {
        this.avatar = Strings.nullToEmpty(avatar);
    }
}
