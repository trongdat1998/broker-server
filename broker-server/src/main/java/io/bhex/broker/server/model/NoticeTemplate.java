/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.model
 *@Date 2018/9/9
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.model;

import com.google.common.base.Strings;
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
@Table(name = "tb_notice_template")
public class NoticeTemplate {

    @Id
    private Long id;
    private Long orgId;
    private Integer noticeType;
    private String businessType;
    private String language;
    private Long templateId;
    private String templateContent;
    private Integer sendType;
    private String sign;
    private String subject;
    private Long created;
    private Long updated;

    public String getUniqueKey() {
        return this.getOrgId() + "_"
                + this.getNoticeType() + "_"
                + this.getBusinessType() + "_"
                + this.getLanguage();
    }

    public io.bhex.broker.grpc.notice.NoticeTemplate toGrpcData() {
        io.bhex.broker.grpc.notice.NoticeTemplate.SendType sendType =
                io.bhex.broker.grpc.notice.NoticeTemplate.SendType.forNumber(this.getSendType());
        return io.bhex.broker.grpc.notice.NoticeTemplate.newBuilder()
                .setOrgId(this.getOrgId())
                .setNoticeType(this.getNoticeType())
                .setBusinessType(this.getBusinessType().toUpperCase())
                .setLanguage(this.getLanguage())
                .setTemplateId(this.getTemplateId())
                .setTemplateContent(this.getTemplateContent())
                .setSendType(sendType == null ? io.bhex.broker.grpc.notice.NoticeTemplate.SendType.AWS : sendType)
                .setSign(Strings.nullToEmpty(this.getSign()))
                .setSubject(Strings.nullToEmpty(this.getSubject()))
                .build();
    }

}
