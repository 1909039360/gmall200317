package com.atguigu.gmallpublisher.servicer;

import java.util.List;
import java.util.Map;

/**
 * Author: doubleZ
 * Datetime:2020/8/17   11:16
 * Description:
 */
public interface PublisherService {
    public int getDauTotal(String date);

    public Map getDauTotalHourMap(String date);

    public  Double getOrderAmountTotal(String date);

    public Map getOrderAmountHourMap(String date);
    public Map getSaleDetail(String date,int startPage,int size,String keyword);

}
