package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_sub_business_subject")
public class SubBusinessSubject {

    @Id
    private Long id;
    private Long orgId;
    private Integer parentSubject; // 父流水类型
    private Integer subject; // 流水类型
    private String subjectName; // 流水名称
    private String language; // 语言
    private Integer status; // 状态  1 启用  0 禁用
    private Long created;
    private Long updated;

}
