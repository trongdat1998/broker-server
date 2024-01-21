package io.bhex.broker.server.domain;

public enum FindPwdStatus {

    ERROR_VERIFY_CODE(-1), // 验证码错误
    WAIT_2FA(-2), // 等待2fa验证
    CHANGE_FIND_TYPE(-3), // 更换找回方式
    ERROR_2FA_VERIFY_CODE(-4), // 错误的2fa
    PASSED_2FA_CHECK(-5), // 通过2fa验证
    SKIP_2FA(0),
    SUCCESS(1);

    private int status;

    FindPwdStatus(int status) {
        this.status = status;
    }

    public int status() {
        return status;
    }

}
