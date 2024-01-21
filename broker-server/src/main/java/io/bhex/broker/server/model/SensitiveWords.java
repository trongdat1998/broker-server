package io.bhex.broker.server.model;

import java.util.Date;

import javax.persistence.Table;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lizhen
 * @date 2018-12-03
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_sensitive_words")
public class SensitiveWords {

    private Long id;
    /**
     * 敏感词
     */
    private String words;
    /**
     * 类型
     */
    private Integer type;
    /**
     * 状态（0可用，-1不可用）
     */
    private Integer status;
    /**
     * 创建时间
     */
    private Date createDate;
}
