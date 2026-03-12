package com.stz.order.sink;

import com.alibaba.fastjson2.JSON;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.stz.order.model.MerchantStats;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * 商户统计 RabbitMQ Sink
 * 将实时商户统计数据发送到 RabbitMQ 队列
 */
@Slf4j
public class MerchantStatsRabbitMQSink extends RichSinkFunction<MerchantStats> {

    private static final long serialVersionUID = 1L;

    // RabbitMQ 连接参数
    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final String virtualHost;
    private final String queueName;
    private final boolean durableQueue;

    // RabbitMQ 连接对象
    private transient ConnectionFactory factory;
    private transient Connection connection;
    private transient Channel channel;

    // 重试配置
    private final int maxRetries;
    private final long retryIntervalMillis;

    /**
     * 完整构造函数
     *
     * @param host               RabbitMQ 主机
     * @param port               RabbitMQ 端口
     * @param username           用户名
     * @param password           密码
     * @param virtualHost        虚拟主机
     * @param queueName          队列名称
     * @param durableQueue       是否持久化队列
     * @param maxRetries         最大重试次数
     * @param retryIntervalMillis 重试间隔（毫秒）
     */
    public MerchantStatsRabbitMQSink(String host, int port, String username, String password,
                                     String virtualHost, String queueName, boolean durableQueue,
                                     int maxRetries, long retryIntervalMillis) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.virtualHost = virtualHost;
        this.queueName = queueName;
        this.durableQueue = durableQueue;
        this.maxRetries = maxRetries;
        this.retryIntervalMillis = retryIntervalMillis;
    }

    /**
     * 简化构造函数（使用默认虚拟主机和队列配置）
     *
     * @param host     RabbitMQ 主机
     * @param port     RabbitMQ 端口
     * @param username 用户名
     * @param password 密码
     * @param queueName 队列名称
     */
    public MerchantStatsRabbitMQSink(String host, int port, String username, String password, String queueName) {
        this(host, port, username, password, "/", queueName, true, 3, 1000L);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        log.info("正在初始化 RabbitMQ Sink...");

        // 初始化连接工厂
        factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setVirtualHost(virtualHost);
        // 连接超时设置
        factory.setConnectionTimeout(30000);
        factory.setHandshakeTimeout(30000);
        factory.setNetworkRecoveryInterval(5000);

        // 建立连接和通道
        connectWithRetry();
        log.info("RabbitMQ Sink 初始化完成 - 队列: {}@{}/{}", queueName, host, virtualHost);
    }

    /**
     * 带重试的连接方法
     */
    private void connectWithRetry() throws Exception {
        int attempt = 0;
        while (attempt <= maxRetries) {
            try {
                connection = factory.newConnection();
                channel = connection.createChannel();

                // 声明队列（如果不存在则创建）
                channel.queueDeclare(queueName, durableQueue, false, false, null);
                log.info("成功连接到 RabbitMQ，队列 '{}' 已声明", queueName);
                return;
            } catch (Exception e) {
                attempt++;
                if (attempt > maxRetries) {
                    log.error("连接到 RabbitMQ 失败，已达到最大重试次数 {}: {}", maxRetries, e.getMessage());
                    throw e;
                }
                log.warn("连接到 RabbitMQ 失败，第 {} 次重试 ({}ms 后): {}",
                        attempt, retryIntervalMillis, e.getMessage());
                TimeUnit.MILLISECONDS.sleep(retryIntervalMillis);
            }
        }
    }

    @Override
    public void invoke(MerchantStats stats, Context context) throws Exception {
        if (stats == null) {
            log.warn("收到空统计数据，跳过发送到 RabbitMQ");
            return;
        }

        try {
            // 将 MerchantStats 转换为 JSON
            String message = JSON.toJSONString(stats);
            byte[] messageBody = message.getBytes(StandardCharsets.UTF_8);

            // 发送消息到队列
            channel.basicPublish("", queueName, null, messageBody);

            if (log.isDebugEnabled()) {
                log.debug("已发送商户统计到 RabbitMQ - 商户: {}, 净额: {}, 队列: {}",
                        stats.getMchNo(), stats.getNetAmount(), queueName);
            }

        } catch (Exception e) {
            log.error("发送消息到 RabbitMQ 失败 - 商户: {}, 错误: {}",
                    stats.getMchNo(), e.getMessage(), e);
            // 尝试重新连接
            reconnectIfNeeded();
            throw e;
        }
    }

    /**
     * 检查连接状态并在需要时重新连接
     */
    private void reconnectIfNeeded() {
        try {
            if (channel == null || !channel.isOpen() || connection == null || !connection.isOpen()) {
                log.warn("RabbitMQ 连接已断开，尝试重新连接...");
                closeResources();
                connectWithRetry();
                log.info("RabbitMQ 连接已恢复");
            }
        } catch (Exception e) {
            log.error("重新连接 RabbitMQ 失败: {}", e.getMessage(), e);
        }
    }

    /**
     * 关闭资源
     */
    private void closeResources() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        } catch (Exception e) {
            log.warn("关闭 RabbitMQ 通道时出错: {}", e.getMessage());
        }

        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (Exception e) {
            log.warn("关闭 RabbitMQ 连接时出错: {}", e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        log.info("正在关闭 RabbitMQ Sink...");
        closeResources();
        super.close();
        log.info("RabbitMQ Sink 已关闭");
    }

    /**
     * 格式化连接信息（用于日志）
     */
    @Override
    public String toString() {
        return String.format(
                "MerchantStatsRabbitMQSink{host='%s', port=%d, queue='%s', virtualHost='%s'}",
                host, port, queueName, virtualHost
        );
    }
}