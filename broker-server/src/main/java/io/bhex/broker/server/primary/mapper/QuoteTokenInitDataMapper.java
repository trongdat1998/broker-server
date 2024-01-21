package io.bhex.broker.server.primary.mapper;

        import io.bhex.broker.server.model.QuoteTokenInitData;
        import org.springframework.stereotype.Component;
        import tk.mybatis.mapper.common.Mapper;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 27/11/2018 4:06 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface QuoteTokenInitDataMapper extends Mapper<QuoteTokenInitData> {

}
