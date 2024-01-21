package io.bhex.broker.server.config;

import io.bhex.base.mysql.BHMysqlDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import tk.mybatis.mapper.autoconfigure.SpringBootVFS;
import tk.mybatis.spring.annotation.MapperScan;

import javax.sql.DataSource;

/**
 * @author wangshouchao
 */
@Slf4j
@Configuration
@MapperScan(basePackages = "io.bhex.broker.server.statistics.clear.mapper", sqlSessionTemplateRef = "clearSqlSessionTemplate")
public class ClearDatasourceConfig {

    public ClearDatasourceConfig() {
    }

    @Bean(name = "clearDatasourceProperties")
    @ConfigurationProperties(prefix = "spring.datasource.clear")
    public DataSourceProperties clearDatasourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = "clearDatasource")
    @ConfigurationProperties(prefix = "spring.datasource.clear.hikari")
    public DataSource clearDatasource(@Qualifier("clearDatasourceProperties") DataSourceProperties dataSourceProperties) {
        return dataSourceProperties.initializeDataSourceBuilder().type(BHMysqlDataSource.class).build();
    }

    @Bean(name = "clearSqlSessionFactory")
    public SqlSessionFactory clearSqlSessionFactory(@Qualifier("clearDatasource") DataSource dataSource) throws Exception {
        SqlSessionFactoryBean factory = new SqlSessionFactoryBean();
        factory.setVfs(SpringBootVFS.class);
        factory.setDataSource(dataSource);
        org.apache.ibatis.session.Configuration configuration = new org.apache.ibatis.session.Configuration();
        configuration.setMapUnderscoreToCamelCase(true);
        factory.setConfiguration(configuration);
        return factory.getObject();
    }

    @Bean(name = "clearSqlSessionTemplate")
    public SqlSessionTemplate clearSqlSessionTemplate(@Qualifier("clearSqlSessionFactory") SqlSessionFactory sqlSessionFactory) {
        return new SqlSessionTemplate(sqlSessionFactory);
    }

    @Bean(name = "clearJdbcTemplate")
    public JdbcTemplate clearJdbcTemplate(@Qualifier("clearDatasource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

}
