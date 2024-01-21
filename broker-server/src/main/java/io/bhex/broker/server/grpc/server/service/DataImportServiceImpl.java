package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.user.RegisterResponse;
import io.bhex.broker.server.domain.RegisterType;
import io.bhex.broker.server.grpc.server.DataImportGrpcService;
import io.bhex.broker.server.model.ImportUserLog;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.broker.server.primary.mapper.ImportUserLogMapper;
import io.bhex.broker.server.primary.mapper.UserVerifyMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Service
@Slf4j
public class DataImportServiceImpl implements DataImportGrpcService {

    @Resource
    private UserService userService;

    @Resource
    private ImportUserLogMapper importUserLogMapper;

    @Resource
    private UserVerifyMapper userVerifyMapper;

    @Transactional(rollbackFor = Exception.class)
    @Override
    public ImportResult importUser(Long originUid, Long orgId, String nationalCode, String mobile, String email, Long inviteUserId, Boolean checkInviteInfo, String comment, Boolean importKyc, Boolean isKyc, Boolean forceCheckEmail) {

        log.info("import user date,orgId={}, nationalCode={}, mobile={}, email={}, inviteUserId={}",
                orgId, nationalCode, mobile, email, inviteUserId);
        String identity = null;
        RegisterType rt = null;
        if (Strings.isNullOrEmpty(mobile) && Strings.isNullOrEmpty(email)) {
            log.error("Mobile and Email is null...");
            return ImportResult.faild();
        }

        if (Strings.isNullOrEmpty(mobile)) {
            identity = email;
            rt = RegisterType.EMAIL;
        }

        if (Strings.isNullOrEmpty(email)) {
            identity = mobile;
            rt = RegisterType.MOBILE;
        }
        String inviteCode = "";
        Long bhopInviteUid = 0L;
        if (Objects.nonNull(inviteUserId) && inviteUserId > 0L) {
            Long inviteUidBhop = findBhopInvitorId(orgId, inviteUserId);
            if (Objects.nonNull(inviteUidBhop)) {
                User user = userService.getUser(inviteUidBhop);
                inviteCode = user.getInviteCode();
                bhopInviteUid = user.getUserId();
            }
        }

        Header header = Header.newBuilder().setOrgId(orgId).setRemoteIp("127.0.0.1").build();
        String password = identity + UUID.randomUUID().toString().replace("-", "");

        try {

            if (isExist(orgId, originUid, identity)) {
                log.info("Imported success... ");
                return ImportResult.faild();
            }

            RegisterResponse resp = userService.importRegister(header, nationalCode, mobile, email, password, inviteCode, bhopInviteUid, false, checkInviteInfo, forceCheckEmail);
            if (resp.getUser() == null) {
                log.warn("import user fail...");
                return ImportResult.faild();
            }

            saveUserImportLog(resp.getUser().getUserId(), orgId, originUid, identity, comment);

            Long userId = resp.getUser().getUserId();
            log.info("import user success,userId={}", userId);

            if (importKyc && isKyc) {
                try {
                    userVerifyMapper.insertSelective(UserVerify.builder()
                            .orgId(orgId)
                            .userId(userId)
                            .nationality(1L)
                            .firstName("")
                            .secondName("")
                            .gender(1)
                            .cardType(1)
                            .cardNo("")
                            .cardFrontUrl("")
                            .cardHandUrl("")
                            .passedIdCheck(0)
                            .verifyStatus(2)
                            .created(System.currentTimeMillis())
                            .updated(System.currentTimeMillis())
                            .build());
                } catch (Exception e) {
                    log.error("import user kyc error", e);
                }
            }

            return ImportResult.success(resp.getUser().getUserId(), resp.getUser().getDefaultAccountId());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return ImportResult.faild();
        }

    }

    private Long findBhopInvitorId(Long orgId, Long inviteUserId) {

        Example exp = new Example(ImportUserLog.class);
        exp.createCriteria().andEqualTo("orgId", orgId)
                .andEqualTo("origUid", inviteUserId);

        List<ImportUserLog> list = importUserLogMapper.selectByExample(exp);
        if (CollectionUtils.isEmpty(list)) {
            return null;
        }

        if (list.size() != 1) {
            throw new IllegalStateException("Record is not unique,orgId=" + orgId + ",inviteUserId=" + inviteUserId);
        }

        return list.get(0).getBhopUid();

    }

    private void saveUserImportLog(Long bhopUid, Long orgId, Long originUid, String identity, String comment) {

        ImportUserLog iul = ImportUserLog.builder().bhopUid(bhopUid).comment(comment).orgId(orgId).origUid(originUid).identity(identity).build();
        int row = importUserLogMapper.insertSelective(iul);
        if (row != 1) {
            throw new IllegalStateException("save import user log fail,orgId=" + orgId + ",identity=" + identity);
        }
    }

    private boolean isExist(Long orgId, Long originUid, String identity) {
        Example exp = new Example(ImportUserLog.class);
        Example.Criteria criteria = exp.createCriteria().andEqualTo("orgId", orgId);
        if (Objects.nonNull(originUid)) {
            criteria.andEqualTo("origUid", originUid);
        }

        if (!Strings.isNullOrEmpty(identity)) {
            criteria.andEqualTo("identity", identity);
        }

        int rows = importUserLogMapper.selectCountByExample(exp);
        if (rows > 1) {
            throw new IllegalStateException("record is not unique,orgId=" + orgId + ",originUid"
                    + nullToDefault(originUid, 0L) + ",identity=" + nullToDefault(identity, ""));
        }

        return rows == 1;
    }

    private <T> T nullToDefault(T object, T defaultValue) {
        if (Objects.isNull(object)) {
            return defaultValue;
        }

        return object;
    }
}
