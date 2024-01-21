package io.bhex.broker.server.config;

import io.bhex.base.mysql.BHMysqlDataSource;
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

@Configuration
@MapperScan(basePackages = "io.bhex.broker.server.statistics.statistics.mapper", sqlSessionTemplateRef = "statisticsSqlSessionTemplate")
public class StatisticsDatasourceConfig {

    public StatisticsDatasourceConfig() {
    }

    @Bean(name = "statisticsDatasourceProperties")
    @ConfigurationProperties(prefix = "spring.datasource.statistics")
    public DataSourceProperties statisticsDatasourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = "statisticsDatasource")
    @ConfigurationProperties(prefix = "spring.datasource.statistics.hikari")
    public DataSource statisticsDatasource(@Qualifier("statisticsDatasourceProperties") DataSourceProperties dataSourceProperties) {
        return dataSourceProperties.initializeDataSourceBuilder().type(BHMysqlDataSource.class).build();
//        return DataSourceBuilder.create().type(BHMysqlDataSource.class).build();
    }

    @Bean(name = "statisticsSqlSessionFactory")
    public SqlSessionFactory statisticsSqlSessionFactory(@Qualifier("statisticsDatasource") DataSource dataSource) throws Exception {
        SqlSessionFactoryBean factory = new SqlSessionFactoryBean();
        factory.setVfs(SpringBootVFS.class);
        factory.setDataSource(dataSource);
        org.apache.ibatis.session.Configuration configuration = new org.apache.ibatis.session.Configuration();
        configuration.setMapUnderscoreToCamelCase(true);
        factory.setConfiguration(configuration);
        return factory.getObject();
    }

    @Bean(name = "statisticsSqlSessionTemplate")
    public SqlSessionTemplate statisticsSqlSessionTemplate(@Qualifier("statisticsSqlSessionFactory") SqlSessionFactory sqlSessionFactory) throws Exception {
        return new SqlSessionTemplate(sqlSessionFactory);
    }

    @Bean(name = "statisticsJdbcTemplate")
    public JdbcTemplate statisticsJdbcTemplate(@Qualifier("statisticsDatasource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

}
