package com;

import com.huaban.analysis.jieba.JiebaSegmenter;

/**
 * Created by wangqi08 on 28/6/2016.
 */
public class Main {
    public static void main(String[] args) {
        JiebaSegmenter segmenter = new JiebaSegmenter();
        String[] sentences =
                new String[]{"【天猫超市】蒙牛 成人奶粉 全脂甜奶粉 400g/袋 早餐 全家营"};
        for (String sentence : sentences) {
            System.out.println(segmenter.process(sentence, JiebaSegmenter.SegMode.INDEX).toString());
        }
    }
}
