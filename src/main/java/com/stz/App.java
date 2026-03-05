package com.stz;

import lombok.extern.slf4j.Slf4j;

/**
 * Hello world!
 *
 */
@Slf4j
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        
        // 测试日志输出
        log.info("========== 开始测试 Logback 日志输出 ==========");
        log.debug("这是一条 DEBUG 日志");
        log.info("这是一条 INFO 日志");
        log.warn("这是一条 WARN 日志");
        log.error("这是一条 ERROR 日志");
        log.info("日志输出测试完成，请检查以下位置：");
        log.info("1. 控制台输出");
        log.info("2. logs/flink-cdc-all.log");
        log.info("3. logs/flink-cdc-error.log");
        log.info("4. logs/flink-cdc-data.log");
        log.info("========== 测试结束 ==========");
    }
}
