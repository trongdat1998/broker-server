package io.bhex.broker.server.grpc.server;

import lombok.Builder;
import lombok.Data;

public interface DataImportGrpcService {

    @Data
    @Builder
    class ImportResult {

        Boolean success;
        Long userId;
        Long accountId;

        public static ImportResult faild() {
            return ImportResult.builder().success(false).build();
        }

        public static ImportResult success(Long userId, Long accountId) {
            return ImportResult.builder().success(true).userId(userId).accountId(accountId).build();
        }

    }

    ImportResult importUser(Long originUid, Long orgId, String nationalCode, String mobile, String email, Long inviteUserId, Boolean checkInviteInfo,
                            String comment, Boolean importKyc, Boolean isKyc, Boolean forceCheckEmail);
}
