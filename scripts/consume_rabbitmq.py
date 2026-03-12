#!/usr/bin/env python3
"""
RabbitMQ 消费者测试脚本
用于消费并打印 Flink 应用发送的商户统计数据

安装依赖：
    pip install pika

使用方式：
    1. 持续监听模式（默认）：
       python consume_rabbitmq.py

    2. 消费指定数量消息后退出：
       python consume_rabbitmq.py --count 10

    3. 自定义连接参数：
       python consume_rabbitmq.py --host 192.168.1.100 --port 5673 --username admin --password secret
"""

import json
import time
import argparse
import sys
from datetime import datetime
from typing import Optional, Dict, Any

try:
    import pika
except ImportError:
    print("错误: 请先安装 pika 库: pip install pika")
    sys.exit(1)


class RabbitMQConsumer:
    """RabbitMQ 消费者类"""

    def __init__(self, host: str = '192.168.0.99', port: int = 5672,
                 username: str = 'admin', password: str = 'StrongPassword123',
                 virtual_host: str = '/', queue_name: str = 'merchant-stats-queue'):
        """
        初始化 RabbitMQ 消费者

        Args:
            host: RabbitMQ 主机地址
            port: RabbitMQ 端口
            username: 用户名
            password: 密码
            virtual_host: 虚拟主机
            queue_name: 队列名称
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.queue_name = queue_name

        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None

    def connect(self) -> bool:
        """连接到 RabbitMQ 服务器"""
        try:
            # 创建连接参数
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.virtual_host,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )

            print(f"正在连接到 RabbitMQ: {self.host}:{self.port}/{self.virtual_host}")
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            # 声明队列（确保队列存在）
            self.channel.queue_declare(
                queue=self.queue_name,
                durable=True
            )

            # 设置 QoS（服务质量）
            self.channel.basic_qos(prefetch_count=1)

            print(f"✓ 成功连接到 RabbitMQ")
            print(f"✓ 队列: {self.queue_name}")
            print("-" * 80)
            return True

        except Exception as e:
            print(f"✗ 连接 RabbitMQ 失败: {e}")
            return False

    def format_message(self, stats: Dict[str, Any]) -> str:
        """
        格式化商户统计消息

        Args:
            stats: 商户统计数据字典

        Returns:
            格式化后的字符串
        """
        # 金额格式化（分转元）
        def format_amount(fen: Optional[int]) -> str:
            if fen is None:
                return "0.00"
            return f"¥{fen/100:.2f}"

        # 时间戳格式化
        def format_timestamp(ts: Optional[int]) -> str:
            if ts is None:
                return "未知时间"
            try:
                dt = datetime.fromtimestamp(ts / 1000)  # 毫秒转秒
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                return str(ts)

        mch_no = stats.get('mchNo', '未知商户')
        event_type = stats.get('eventType', '未知类型')
        update_time = format_timestamp(stats.get('updateTime'))

        # 金额数据
        total_amount = format_amount(stats.get('totalAmount'))
        success_amount = format_amount(stats.get('successAmount'))
        refund_amount = format_amount(stats.get('refundAmount'))
        cancel_amount = format_amount(stats.get('cancelAmount'))
        net_amount = format_amount(stats.get('netAmount'))

        # 构建格式化输出
        lines = [
            f"【商户统计】{datetime.now().strftime('%H:%M:%S')}",
            f"商户号: {mch_no}",
            f"事件类型: {event_type}",
            f"更新时间: {update_time}",
            f"总金额: {total_amount}",
            f"成功金额: {success_amount}",
            f"退款金额: {refund_amount}",
            f"撤销金额: {cancel_amount}",
            f"净额: {net_amount}",
            f"原始数据: {json.dumps(stats, ensure_ascii=False)}"
        ]

        return "\n".join(lines)

    def consume_single_message(self) -> bool:
        """
        消费单条消息

        Returns:
            是否成功消费到消息
        """
        if not self.channel:
            return False

        try:
            # 获取单条消息（不自动确认）
            method_frame, header_frame, body = self.channel.basic_get(
                queue=self.queue_name,
                auto_ack=False
            )

            if method_frame:
                # 解析消息
                try:
                    message = body.decode('utf-8')
                    stats = json.loads(message)

                    # 格式化并打印
                    print(self.format_message(stats))
                    print("-" * 80)

                    # 确认消息已处理
                    self.channel.basic_ack(method_frame.delivery_tag)
                    return True

                except json.JSONDecodeError as e:
                    print(f"✗ JSON 解析失败: {e}")
                    print(f"原始消息: {body[:100]}...")
                    # 拒绝消息（不重新入队）
                    self.channel.basic_reject(method_frame.delivery_tag, requeue=False)
                except Exception as e:
                    print(f"✗ 处理消息失败: {e}")
                    # 拒绝消息（重新入队）
                    self.channel.basic_reject(method_frame.delivery_tag, requeue=True)

            return False

        except Exception as e:
            print(f"✗ 消费消息失败: {e}")
            return False

    def consume_continuous(self, max_count: Optional[int] = None):
        """
        持续消费消息

        Args:
            max_count: 最大消费数量，None 表示无限
        """
        if not self.channel:
            return

        print(f"开始监听队列 '{self.queue_name}'...")
        print("按 Ctrl+C 停止")
        print("=" * 80)

        count = 0
        try:
            while True:
                if max_count is not None and count >= max_count:
                    print(f"\n✓ 已达到最大消费数量: {max_count}")
                    break

                if self.consume_single_message():
                    count += 1
                else:
                    # 没有消息，等待一下
                    time.sleep(1)

        except KeyboardInterrupt:
            print(f"\n\n✂ 用户中断，已消费 {count} 条消息")
        except Exception as e:
            print(f"\n✗ 消费过程中出现错误: {e}")
        finally:
            self.close()

    def close(self):
        """关闭连接"""
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                print("✓ RabbitMQ 连接已关闭")
        except Exception as e:
            print(f"✗ 关闭连接时出错: {e}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='RabbitMQ 消费者测试脚本 - 消费 Flink 商户统计数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # 连接参数
    parser.add_argument('--host', default='192.168.0.99', help='RabbitMQ 主机地址 (默认: localhost)')
    parser.add_argument('--port', type=int, default=5672, help='RabbitMQ 端口 (默认: 5672)')
    parser.add_argument('--username', default='admin', help='RabbitMQ 用户名 (默认: guest)')
    parser.add_argument('--password', default='StrongPassword123', help='RabbitMQ 密码 (默认: guest)')
    parser.add_argument('--virtual-host', default='/', help='RabbitMQ 虚拟主机 (默认: /)')
    parser.add_argument('--queue', default='merchant-stats-queue',
                       help='队列名称 (默认: merchant-stats-queue)')

    # 消费模式参数
    parser.add_argument('--count', type=int, default=None,
                       help='消费指定数量消息后退出 (默认: 持续监听)')
    parser.add_argument('--timeout', type=int, default=None,
                       help='超时时间(秒)，超过后自动退出 (默认: 无超时)')

    # 其他参数
    parser.add_argument('--test', action='store_true',
                       help='测试模式，只检查连接不消费消息')

    args = parser.parse_args()

    # 创建消费者
    consumer = RabbitMQConsumer(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        virtual_host=args.virtual_host,
        queue_name=args.queue
    )

    # 连接 RabbitMQ
    if not consumer.connect():
        sys.exit(1)

    # 测试模式
    if args.test:
        print("✓ 连接测试成功")
        consumer.close()
        return

    # 设置超时（如果有）
    if args.timeout:
        import threading

        def timeout_handler():
            print(f"\n⏰ 超时 {args.timeout} 秒，自动退出")
            consumer.close()
            sys.exit(0)

        timer = threading.Timer(args.timeout, timeout_handler)
        timer.daemon = True
        timer.start()

    # 开始消费
    try:
        consumer.consume_continuous(max_count=args.count)
    except Exception as e:
        print(f"✗ 消费过程中出现错误: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()