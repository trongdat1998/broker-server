package io.bhex.broker.server.util;

import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

public class UrlBuilder {

    private String urlTemplate;

    private final MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

    private UrlBuilder(String urlTemplate) {
        this.urlTemplate = urlTemplate;
    }

    public static UrlBuilder fromUrlTemplate(String urlTemplate) {
        return new UrlBuilder(urlTemplate);
    }

    public UrlBuilder addParams(String key, String value) {
        if (value != null && !value.equals("")) {
            params.add(key, value);
        }
        return this;
    }

    public String build() {
        UriComponents uriComponents = UriComponentsBuilder.fromHttpUrl(urlTemplate).queryParams(params).build();
        return uriComponents.toString();
    }
}
