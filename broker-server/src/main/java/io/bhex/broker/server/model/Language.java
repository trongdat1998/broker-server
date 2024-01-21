package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_language")
public class Language {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private String name;

    private String sign;

    private Integer status;

    private Long createdAt;

    private Long updatedAt;


}
