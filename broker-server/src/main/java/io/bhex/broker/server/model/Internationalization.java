package io.bhex.broker.server.model;

import java.sql.Timestamp;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: yuehao  <hao.yue@bhex.com>
 * @CreateDate: 2019-03-08 12:00
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_internationalization")
public class Internationalization {

    @Id
    private Long id;

    private String inKey;

    private String inValue;

    private String env;

    private Integer type;

    private Integer inStatus;

    private Timestamp createAt;
}
