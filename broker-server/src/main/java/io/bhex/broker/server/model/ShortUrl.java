package io.bhex.broker.server.model;

import java.util.Date;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 短链接
 *
 * @author lizhen
 * @date 2018-11-27
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_short_url")
public class ShortUrl {
    /**
     * ID
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * 长链接
     */
    private String longUrl;
    /**
     * 长链接md5
     */
    private String md5Value;
    /**
     * 状态（0可用，-1不可用）
     */
    private Integer status;
    /**
     * 创建时间
     */
    private Date createDate;
}
