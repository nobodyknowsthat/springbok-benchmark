package com.anonymous.test.benchmark.predicate;

import java.util.List;

/**
 * @author anonymous
 * @create 2022-06-29 10:38 AM
 **/
public class StatisticUtil {
    public static double calculateAverage(List<Integer> valueList) {
        if (valueList.size() == 0) {
            return -1;
        }
        double sum = 0;
        for (int value : valueList) {
            sum = sum + value;
        }

        return sum / valueList.size();
    }
}
