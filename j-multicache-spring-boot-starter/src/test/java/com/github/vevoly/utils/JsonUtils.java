package com.github.vevoly.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@UtilityClass
public class JsonUtils {

    /**
     * 序列化，将对象转化为json字符串
     *
     * @param data
     * @return
     */
    public static String toJsonString(Object data) {
        ObjectMapper mapper = SpringContextHolder.getBean(ObjectMapper.class);
        if (data == null) {
            return null;
        }

        String json = null;
        try {
            json = mapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            log.error("[{}] toJsonString error：{{}}", data.getClass().getSimpleName(), e);
        }
        return json;
    }


    /**
     * 反序列化，将json字符串转化为对象
     *
     * @param json
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> T parse(@NonNull String json, Class<T> clazz) {
        ObjectMapper mapper = SpringContextHolder.getBean(ObjectMapper.class);
        T t = null;
        try {
            t = mapper.readValue(json, clazz);
        } catch (Exception e) {
            log.error(" parse json [{}] to class [{}] error：{{}}", json, clazz.getSimpleName(), e);
        }
        return t;
    }


    /**
     * 反序列化，将json字符串转化为list对象
     * @param json
     * @param clazz
     * @return
     * @param <T>
     */
    public static <T> List<T> parseArray(@NonNull String json, Class<T> clazz) {
        ObjectMapper mapper = SpringContextHolder.getBean(ObjectMapper.class);
        try {
            return mapper.readValue(json, mapper.getTypeFactory().constructCollectionType(List.class, clazz));
        } catch (Exception e) {
            log.error(" parse json [{}] to list [{}] error：{{}}", json, clazz.getSimpleName(), e);
        }
        return null;
    }
}
