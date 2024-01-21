/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.mapper
 *@Date 2018/6/25
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.primary.mapper;

import com.google.common.base.Strings;
import io.bhex.broker.server.model.User;
import org.apache.ibatis.jdbc.SQL;
import org.springframework.util.StringUtils;

import java.util.Map;

public class UserSqlProvider {

    private static final String TABLE_NAME = "tb_user";

    private static final String COLUMNS = "id, org_id, user_id, bh_user_id, national_code, mobile, concat_mobile, email, register_type, nickname, avatar, ip, user_status, "
            + "input_invite_code, invite_user_id, second_level_invite_user_id, invite_code, bind_ga, bind_trade_pwd, bind_password, auth_type, anti_phishing_code, user_type, channel, source, "
            + "platform, user_agent, language, app_base_header, created, updated, is_vip, is_virtual, custom_label_ids";

    public String getByUserId() {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("user_id = #{userId}");
            }
        }.toString();
    }

    public String getByOrgAndUserId() {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("org_id = #{orgId}").WHERE("user_id = #{userId}");
            }
        }.toString();
    }

    public String getByEmail() {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("org_id = #{orgId}").WHERE("email = #{email}");
            }
        }.toString();
    }

    public String getByEmailAlias() {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("org_id = #{orgId}").WHERE("email_alias = #{emailAlias}");
            }
        }.toString();
    }

    public String getByMobile(Map<String, Object> parameter) {
        String nationalCode = (String) parameter.get("nationalCode");
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("org_id = #{orgId}");
                if (!Strings.isNullOrEmpty(nationalCode)) {
                    WHERE("national_code = #{nationalCode}");
                }
                WHERE("mobile = #{mobile}");
            }
        }.toString() + " LIMIT 1";
    }

    public String getByUsername() {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("org_id = #{orgId} AND (mobile = #{username} OR concat_mobile = #{username} OR email = #{username} OR email_alias = #{username})");
            }
        }.toString();
    }

    public String getByInviteCode() {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("org_id = #{orgId}").WHERE("invite_code = #{inviteCode}");
            }
        }.toString();
    }

    public String insert(User user) {
        return new SQL() {
            {
                INSERT_INTO(TABLE_NAME);
                if (user.getOrgId() != null) {
                    VALUES("org_id", "#{orgId}");
                }
                if (user.getUserId() != null) {
                    VALUES("user_id", "#{userId}");
                }
                if (user.getBhUserId() != null) {
                    VALUES("bh_user_id", "#{bhUserId}");
                }
                if (!Strings.isNullOrEmpty(user.getNationalCode())) {
                    VALUES("national_code", "#{nationalCode}");
                }
                if (!Strings.isNullOrEmpty(user.getMobile())) {
                    VALUES("mobile", "#{mobile}");
                }
                if (!Strings.isNullOrEmpty(user.getConcatMobile())) {
                    VALUES("concat_mobile", "#{concatMobile}");
                }
                if (!Strings.isNullOrEmpty(user.getEmail())) {
                    VALUES("email", "#{email}");
                }
                if (!Strings.isNullOrEmpty(user.getEmailAlias())) {
                    VALUES("email_alias", "#{emailAlias}");
                }
                if (user.getRegisterType() != null) {
                    VALUES("register_type", "#{registerType}");
                }
                if (!Strings.isNullOrEmpty(user.getIp())) {
                    VALUES("ip", "#{ip}");
                }
                if (!Strings.isNullOrEmpty(user.getInputInviteCode())) {
                    VALUES("input_invite_code", "#{inputInviteCode}");
                }
                if (user.getInviteUserId() != null) {
                    VALUES("invite_user_id", "#{inviteUserId}");
                }
                if (user.getSecondLevelInviteUserId() != null) {
                    VALUES("second_level_invite_user_id", "#{secondLevelInviteUserId}");
                }
                if (!Strings.isNullOrEmpty(user.getInviteCode())) {
                    VALUES("invite_code", "#{inviteCode}");
                }
                if (user.getBindPassword() != null) {
                    VALUES("bind_password", "#{bindPassword}");
                }
                if (!Strings.isNullOrEmpty(user.getChannel())) {
                    VALUES("channel", "#{channel}");
                }
                if (!Strings.isNullOrEmpty(user.getSource())) {
                    VALUES("source", "#{source}");
                }
                if (!Strings.isNullOrEmpty(user.getPlatform())) {
                    VALUES("platform", "#{platform}");
                }
                if (!Strings.isNullOrEmpty(user.getUserAgent())) {
                    VALUES("user_agent", "#{userAgent}");
                }
                if (!Strings.isNullOrEmpty(user.getLanguage())) {
                    VALUES("language", "#{language}");
                }
                if (!Strings.isNullOrEmpty(user.getAppBaseHeader())) {
                    VALUES("app_base_header", "#{appBaseHeader}");
                }
                if (user.getIsVirtual() != null) {
                    VALUES("is_virtual", "#{isVirtual}");
                }
                if (user.getCustomLabelIds() != null) {
                    VALUES("custom_label_ids", "#{customLabelIds}");
                }
                VALUES("created", "#{created}");
                VALUES("updated", "#{updated}");
            }
        }.toString();
    }

    public String update(User user) {
        return new SQL() {
            {
                UPDATE(TABLE_NAME);
                if (!Strings.isNullOrEmpty(user.getNationalCode())) {
                    SET("national_code = #{nationalCode}");
                }
                if (!Strings.isNullOrEmpty(user.getMobile())) {
                    SET("mobile = #{mobile}");
                }
                if (!Strings.isNullOrEmpty(user.getConcatMobile())) {
                    SET("concat_mobile = #{concatMobile}");
                }
                if (!Strings.isNullOrEmpty(user.getEmail())) {
                    SET("email = #{email}");
                }
                if (!Strings.isNullOrEmpty(user.getEmailAlias())) {
                    SET("email_alias = #{emailAlias}");
                }
                if (!Strings.isNullOrEmpty(user.getNickname())) {
                    SET("nickname = #{nickname}");
                }
                if (!Strings.isNullOrEmpty(user.getAvatar())) {
                    SET("avatar = #{avatar}");
                }
                if (user.getInviteUserId() != null && user.getInviteUserId() > 0) {
                    SET("invite_user_id = #{inviteUserId}");
                }
                if (user.getSecondLevelInviteUserId() != null && user.getSecondLevelInviteUserId() > 0) {
                    SET("second_level_invite_user_id = #{secondLevelInviteUserId}");
                }
                if (user.getBindGA() != null) {
                    SET("bind_ga = #{bindGA}");
                }
                if (user.getBindTradePwd() != null) {
                    SET("bind_trade_pwd = #{bindTradePwd}");
                }
                if (user.getBindPassword() != null) {
                    SET("bind_password = #{bindPassword}");
                }
                if (user.getAuthType() != null) {
                    SET("auth_type = #{authType}");
                }
                if (!Strings.isNullOrEmpty(user.getAntiPhishingCode())) {
                    SET("anti_phishing_code = #{antiPhishingCode}");
                }
                if (user.getCustomLabelIds() != null) {
                    SET("custom_label_ids = #{customLabelIds}");
                }
                SET("updated = #{updated}");
                WHERE("user_id = #{userId}");
            }
        }.toString();
    }

    public String listUpdateUserByDate(Map<String, Object> parameter) {
        final Long fromId = (Long) parameter.get("fromId");
        final Long endId = (Long) parameter.get("endId");
        final Long startTime = (Long) parameter.get("startTime");
        final Long endTime = (Long) parameter.get("endTime");
        final Boolean isAsc = true;
        return new SQL() {
            {
                SELECT(COLUMNS);
                FROM(TABLE_NAME);
                WHERE("org_id = #{orgId}");
                if (fromId != null && fromId > 0L) {
                    if (isAsc) {
                        WHERE("id > #{fromId}");
                    } else {
                        WHERE("id < #{endId}");
                    }
                }
                if (endId != null && endId > 0L) {
                    if (isAsc) {
                        WHERE("id < #{fromId}");
                    } else {
                        WHERE("id > #{endId}");
                    }
                }

                if (startTime != null && startTime > 0) {
                    WHERE("updated > #{startTime}");
                }
                if (endTime != null && endTime > 0) {
                    WHERE("updated < #{endTime}");
                }

                if (isAsc) {
                    ORDER_BY("id ASC");
                } else {
                    ORDER_BY("id DESC");
                }
            }
        }.toString() + " LIMIT #{limit}";
    }
}
