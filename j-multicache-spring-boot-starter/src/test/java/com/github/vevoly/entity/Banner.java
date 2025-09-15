package com.github.vevoly.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Banner {

    private long id;

    private Integer status;

    private String showUserType;

}
