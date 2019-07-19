package com.atguigu.bigdata.gmall.mock.util;

import java.util.Random;

/**
 * @author Witzel
 * @since 2019/7/19 18:51
 */
public class RandomNum {

            public static final  int getRandInt(int fromNum,int toNum){
               return   fromNum+ new Random().nextInt(toNum-fromNum+1);
            }
        }