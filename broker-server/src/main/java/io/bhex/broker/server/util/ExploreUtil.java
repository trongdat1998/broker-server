package io.bhex.broker.server.util;

import org.apache.commons.lang3.StringUtils;

public class ExploreUtil {

    public static final String BUSDT = "BUSDT";

    public static final String FILTER_EXPLORE_TRUE = "1";

    public static boolean isFilterExplore(String filterExplore) {
        if (StringUtils.isEmpty(filterExplore)) {
            return false;
        }
        return filterExplore.equalsIgnoreCase(FILTER_EXPLORE_TRUE);
    }

}
