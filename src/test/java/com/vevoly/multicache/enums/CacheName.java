package com.vevoly.multicache.enums;


import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum CacheName {

    USER_DETAILS("user:user_details"),

    ;

    private final String name;
}
