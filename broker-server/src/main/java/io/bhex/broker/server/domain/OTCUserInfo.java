package io.bhex.broker.server.domain;

import com.google.common.base.Strings;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.ex.otc.OTCUser;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Data
@Builder(builderClassName = "builder")
@NoArgsConstructor
@AllArgsConstructor
public class OTCUserInfo {

    private User user;
    private UserVerify verify;
    private OTCUser otcUser;

    public String getRealName(){
        if(Objects.isNull(verify)){
            return "";
        }

        return Strings.nullToEmpty(this.verify.getFirstName())+Strings.nullToEmpty(this.verify.getSecondName());
    }
}
