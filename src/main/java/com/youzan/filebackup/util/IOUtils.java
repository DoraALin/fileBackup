package com.youzan.filebackup.util;

import com.google.gson.Gson;

/**
 * Created by lin on 17/4/7.
 */
public class IOUtils {

    private final static Gson GSON = new Gson();

    public static Gson getGson() {
        return GSON;
    }
}
