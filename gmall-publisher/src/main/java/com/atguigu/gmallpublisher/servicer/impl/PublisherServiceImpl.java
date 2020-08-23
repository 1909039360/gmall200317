package com.atguigu.gmallpublisher.servicer.impl;

import com.atguigu.gmallpublisher.bean.Option;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.servicer.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import sun.plugin.javascript.navig.ImageArray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: doubleZ
 * Datetime:2020/8/17   15:35
 * Description:
 */
//获取日活分时数据
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;
    @Autowired
    private OrderMapper orderMapper;
    @Autowired
    private JestClient jestClient;
    @Override
    public int getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {

        //1.查询Phoneix
        List<Map> list = dauMapper.selectDauTotalHourMap(date);
        //2.创建Map用于存调整结构后的数据
        HashMap<String, Long> result = new HashMap<>();
        //3.调整结构
        for (Map map : list) {
            result.put((String)map.get("LH"),(Long) map.get("CT"));
        }
        //4.返回数据
        return result;
    }
    public  Double getOrderAmountTotal(String date){
        return orderMapper.selectOrderAmountTotal(date);
    }

    public Map getOrderAmountHourMap(String date){
        //1.查询Phoenix
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);
        //2.创建Map用于存放结果数据
        HashMap<String, Double> result = new HashMap<>();
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"),(Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }

    @Override
    public Map getSaleDetail(String date, int startPage, int size, String keyword) {
        //1.创建DSL 语句构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //1.1 查询条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //1.1.1 条件全职匹配选项
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("dt", date);
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND);
        boolQueryBuilder.filter(termQueryBuilder);
        boolQueryBuilder.must(matchQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);
        //性别聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupBy_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);
        //年龄聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupBy_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);
        //1.3 分页相关
        // 行号 =(页面 -1)*每页行数
        searchSourceBuilder.from((startPage - 1)*size);
        searchSourceBuilder.size(size);
        //2.创建Search对象
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("gmall200317_sale_detail-query")
                .addType("_doc")
                .build();
        //3.执行查询
        SearchResult searchResult =null;
        try {
             searchResult = jestClient.execute(search);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //4.解析searchResult
        //4.1 定义Map用于存放最终的结果数据
        HashMap<String, Object> result = new HashMap<>();
        //4.2 获取查询总数
        assert searchResult != null;
        Long total = searchResult.getTotal();
        //4.3 封装年龄以以及性别聚合组数据
        MetricAggregation aggregations = searchResult.getAggregations();
        ArrayList<Stat> stats = new ArrayList<>();
        //4.3.1 年龄聚合组
        TermsAggregation groupByAge = aggregations.getTermsAggregation("groupBy_user_age");
        //定义每个年龄段的人数变量
        Long lower20 = 0L;
        Long upper20to30 = 0L;
        //遍历年龄数据,将各个年龄的数据转换为各个年龄段的数据
        for (TermsAggregation.Entry entry : groupByAge.getBuckets()) {
            long age = Long.parseLong(entry.getKey());
            if(age < 20){
                lower20 += entry.getCount();
            }else if(age < 30){
                upper20to30 += entry.getCount();
            }
        }
        // 将人数转换为比例
        double lower20Ratio = Math.round(lower20 * 1000D / total) / 10D;
        double upper20to30Ratio = Math.round(upper20to30 * 1000D / total) / 10D;
        double upper30Ratio = 100-lower20Ratio-upper20to30Ratio;
        //创建年龄段的Option对象
        Option lower20Opt = new Option("20岁以下", lower20Ratio);
        Option upper20to30Opt = new Option("20岁到30岁", upper20to30Ratio);
        Option upper30Opt = new Option("30岁及30岁以上", upper30Ratio);

        // 创建年龄的Stat对象


        return null;
    }

}
