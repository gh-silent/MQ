using com.np.sz.library.Mq.Active;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.np.sz.test
{
    class TestActiveMQ
    {
        private static event Action<string> _onMessage;

        // 主题
        static string _topic = "cgz.topic";
        // 连接地址
        static string _brokerUrl = "tcp://127.0.0.1:61616";

        /// <summary>
        /// 测试生产者
        /// </summary>
        public static void TestProducer()
        {
            ActiveProducer producer = new ActiveProducer(_topic, _brokerUrl, true);

            // 创建生产者

            // 要发送的消息
            // String strMessage = "报警数据来了";
            // System.out.println("开始发送时间:" + DateHelper.getNowStringDate());


            for (int i = 0; i < 100; ++i)
            {
                string str = "报警数据aaaa" + i;
                // 批量发送文本数据
                producer.SendTextMessage(str);
            }

            List<string> textList = new List<string>();
            textList.Add("报警数据aaaa");
            textList.Add("报警数据bbbb");
            textList.Add("报警数据cccc");
            // 批量发送文本数据
            producer.SendTextListMessage(textList);

            // List<byte[]> byteList = new ArrayList<byte[]>();
            // byteList.add("报警数据4".getBytes("utf-8"));
            // byteList.add("报警数据5".getBytes("utf-8"));
            // byteList.add("报警数据6".getBytes("utf-8"));
            // 批量发送byte[]数据
            // ativeProducer.sendByteListMessage(byteList);

            producer.Stop();
        }

        /// <summary>
        /// 测试消费者
        /// </summary>
        public static void TestConsumer()
        {
            // 数据回调
            _onMessage += OnMessageHandle;
            ActiveConsumer consumer = new ActiveConsumer(_topic, _brokerUrl, _onMessage);
            consumer.Start();
        }

        static void OnMessageHandle(string message)
        {
            Console.WriteLine(message);
        }

    }
}
