/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.domain
 *@Date 2019-02-14
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.domain;

import io.bhex.base.token.TokenDetail;
import io.bhex.broker.server.model.Token;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
public class TokenWithdrawLimitDTO {

    private String tokenId;
    private String feeTokenId;
    private String minerFeeTokenId;
    private boolean allowWithdraw;
    private int precision; // 精度
    private int minerFeePrecision; // 手续费精度
    private BigDecimal maxWithdrawQuota; // 日限额
    private BigDecimal minWithdrawQuantity; // 最小提币数量
    private BigDecimal fee; // 券商收取的手续费
    private boolean isEOS;
    private boolean needAddressTag;
    private Token token;
    private TokenDetail tokenDetail;

}
