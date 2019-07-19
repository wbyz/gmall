package com.atguigu.bigdata.gmall.mock.util;

/**
 * @author Witzel
 * @since 2019/7/19 18:53
 */
public class RanOpt<T> {
    T value;
    int weight;

    public RanOpt(T value, int weight) {
        this.value = value;
        this.weight = weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}
         