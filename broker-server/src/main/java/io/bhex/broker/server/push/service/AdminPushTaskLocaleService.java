package io.bhex.broker.server.push.service;

import io.bhex.broker.server.model.AdminPushTaskLocale;
import io.bhex.broker.server.primary.mapper.AdminPushTaskLocaleMapper;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.List;

/**
 * TbAdminPushTaskLocaleServiceImpl
 * <p>
 * author: wangshouchao
 * Date: 2020/07/25 06:25:01
 */
@Service
public class AdminPushTaskLocaleService {

    @Resource
    private AdminPushTaskLocaleMapper adminPushTaskLocaleMapper;

    /**
     * 根据taskId + language 查找对应语言包
     */
    public AdminPushTaskLocale getPushTaskLocale(Long taskId, String language) {
        Example example = new Example(AdminPushTaskLocale.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("taskId", taskId);
        criteria.andEqualTo("language", language);
        return adminPushTaskLocaleMapper.selectOneByExample(example);
    }

    /**
     * 根据taskId 查找对应语言包
     */
    public List<AdminPushTaskLocale> getPushTaskLocaleList(Long taskId) {
        Example example = new Example(AdminPushTaskLocale.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("taskId", taskId);
        return adminPushTaskLocaleMapper.selectByExample(example);
    }

    public void submit(AdminPushTaskLocale adminPushTaskLocale) {
        adminPushTaskLocaleMapper.insertSelective(adminPushTaskLocale);
    }

    public void removeById(Long id) {
        adminPushTaskLocaleMapper.deleteByPrimaryKey(id);
    }
}