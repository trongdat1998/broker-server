package io.bhex.broker.server.push.bo;

/**
 * 用于缓存精简(只存储发送使用到的数据)
 */
public final class AppPushDeviceSimple implements Comparable<AppPushDeviceSimple>{
    /**
     * 用户设备表对应id
     */
    private final long id;
    /**
     * 设备token
     */
    private final String token;

    public AppPushDeviceSimple(long id,String token){
        this.id = id;
        this.token = token;
    }

    public long getId() {
        return id;
    }

    public String getToken() {
        return token;
    }

    @Override
    public int compareTo(AppPushDeviceSimple simple) {
        if (this.id > simple.id) {
            return 1;
        } else if (this.id == simple.id) {
            return 0;
        } else {
            return -1;
        }
    }
}