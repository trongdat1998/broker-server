package io.bhex.broker.server.domain.kyc.tencent;

import lombok.Data;

@Data
public class IDCardOcrObject {
    private String warning;
    private String ocrId;
    private String orderNo;
    private String name;
    private String sex;
    private String nation;
    private String birth;
    private String idcard;
    private String address;
    private String authority;
    private String validDate;
    private String multiWarning;
    private String clarity;
}
