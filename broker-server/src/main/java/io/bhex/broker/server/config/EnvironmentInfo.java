/*
* ***********************************
 * @项目名称: BlueHelix Exchange Project
 * @文件名称: EnvironmentInfo
 * @Author essen 
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.server.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "env")
public class EnvironmentInfo {

    /**
     * The profile of current engine, like as : prod/test/benchmark
     * <p>
     * In test profile, it will run as mock data
     */
    private String profile;
}
