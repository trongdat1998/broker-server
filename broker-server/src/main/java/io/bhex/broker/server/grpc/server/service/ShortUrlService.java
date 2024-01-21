package io.bhex.broker.server.grpc.server.service;

import java.util.Date;
import java.util.Objects;

import com.qcloud.cos.utils.Md5Utils;
import io.bhex.broker.common.util.Base62Util;
import io.bhex.broker.server.primary.mapper.ShortUrlMapper;
import io.bhex.broker.server.model.ShortUrl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author lizhen
 * @date 2018-11-27
 */
@Slf4j
@Service
public class ShortUrlService {

    @Autowired
    private ShortUrlMapper shortUrlMapper;

    public long createShortUrl(String longUrl) {
        String md5Value = Md5Utils.md5Hex(longUrl.getBytes());
        ShortUrl example = ShortUrl.builder().md5Value(md5Value).build();
        ShortUrl existUrl = shortUrlMapper.selectOne(example);
        if (existUrl != null) {
            return existUrl.getId();
        }
        ShortUrl shortUrl = ShortUrl.builder()
            .longUrl(longUrl)
            .md5Value(md5Value)
            .status(0)
            .createDate(new Date())
            .build();
        shortUrlMapper.insert(shortUrl);
        if (shortUrl.getId() == null || shortUrl.getId() <= 0) {
            return -1;
        }
        return shortUrl.getId();
    }

    public ShortUrl getShortUrl(long id) {
        return shortUrlMapper.selectByPrimaryKey(id);
    }

    public String findShortUrl(String longUrl){
        String md5Value = Md5Utils.md5Hex(longUrl.getBytes());
        ShortUrl example = ShortUrl.builder().md5Value(md5Value).build();
        ShortUrl existUrl = shortUrlMapper.selectOne(example);
        if(Objects.isNull(existUrl)){
            return "";
        }

        long urlId = existUrl.getId();
        if (urlId > 0) {
            return Base62Util.encode(urlId) + Base62Util.getValid(urlId);
        }

        return "";
    }
}
