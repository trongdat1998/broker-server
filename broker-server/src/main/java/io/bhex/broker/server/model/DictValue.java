package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 键值对
 *
 * @author generator
 * @date 2021-01-13
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_dict_values")
public class DictValue {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    /**
     * 机构id
     */
    private Long orgId;

    /**
     * 业务key
     */
    private String bizKey;

    /**
     * 语言
     */
    private String language;

    /**
     * 键值Text
     */
    private String dictText;

    /**
     * 键值
     */
    private Integer dictValue;

    /**
     * 创建时间
     */
    private Long createdAt;

    /**
     * 修改时间
     */
    private Long updatedAt;
}
