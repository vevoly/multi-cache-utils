package com.github.vevoly.enums;


import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum CacheName {

    USER_DETAILS("user:user_details"),
    USER_LIST("user:user_list"),
    USER_SET("user:user_set"),
    USER_PAGE("result:user_page"),

    BANNER_LIST("banner:banner_list"),
    BANNER_USER_TYPE_USER("banner:banner_user_type_user:set"),
    BANNER_USER_TYPE_LEVEL("banner:banner_user_type_level:set"),
    BANNER_USER_TYPE("banner:banner_user_type:union"),

    ;

    private final String namespace;
}
