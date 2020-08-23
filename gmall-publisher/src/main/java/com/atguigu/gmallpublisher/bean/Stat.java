package com.atguigu.gmallpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Author: doubleZ
 * Datetime:2020/8/22   11:44
 * Description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Stat {
    String title;
    List<Option> Options;
}
