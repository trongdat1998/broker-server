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
import java.util.List;
import java.util.stream.Collectors;

/**
 * User Settings Entity
 *
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_user_settings")
public class UserSettings {

    @Id
    private Long id;

    @Column(name = "org_id")
    private Long orgId;

    @Column(name = "user_id")
    private Long userId;

    //通用配置，json的数据格式保存
    @Column(name = "common_config")
    private String commonConfig;

    private Long created;
    private Long updated;
}
