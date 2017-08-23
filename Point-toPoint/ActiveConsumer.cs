using Apache.NMS;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.Util;
using com.np.sz.util.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace com.np.sz.library.Mq.Active
{
    /// <summary>
    /// ActiveMQ消费者
    /// </summary>
    public class ActiveConsumer
    {
        private IConnectionFactory _connectionFactory = null;
        private IConnection _connection = null;
        private ISession _session = null;
        private IDestination _destination = null;
        private IMessageConsumer _consumer = null;
        // 主题
        private string _topic;
        // MQ地址，形如 tcp://192.168.1.34:61616
        private string _brokerUrl;
        // 回调
        private event Action<string> _onMessageHandle = null;
        // 是否持久化订阅
        private bool _isPersistent;
        // 消费者ID(持久化订阅时生效)
        private string _clientID;


        /// <summary>
        /// 普通订阅构造函数
        /// </summary>
        /// <param name="topic">主题</param>
        /// <param name="brokerUrl">MQ地址，形如 tcp://192.168.1.34:61616</param>
        /// <param name="callback">回调</param>
        /// <param name="clientID">消费者ID</param>
        public ActiveConsumer(string topic, string brokerUrl, Action<string> callback)
        {
            this._topic = topic;
            this._brokerUrl = brokerUrl;
            this._onMessageHandle = callback;
        }

        /// <summary>
        /// 持久订阅构造函数
        /// </summary>
        /// <param name="topicName">主题</param>
        /// <param name="brokerUrl">MQ地址，形如 tcp://192.168.1.34:61616</param>
        /// <param name="callback">回调</param>
        /// <param name="isPersistent">是否持久化订阅</param>
        /// <param name="clientID">消费者ID(持久化订阅时生效)</param>
        public ActiveConsumer(string topicName, string brokerUrl, Action<string> callback, bool isPersistent, string clientID)
        {
            this._topic = topicName;
            this._brokerUrl = brokerUrl;
            this._onMessageHandle = callback;
            this._isPersistent = isPersistent;
            this._clientID = clientID;
        }

        /// <summary>
        /// 启动
        /// </summary>
        public void Start()
        {
            this._consumer = CreateConsumer();
            //_isRunning = true;
            //if (_daemonThrd == null || !_daemonThrd.IsAlive)
            //{
            //    _daemonThrd = new Thread(DoWork);
            //    _daemonThrd.IsBackground = true;
            //    _daemonThrd.Name = "activemq-comsumer-daemonThrd" + DateTime.Now.ToString("yyyyMMddHHssmmfff");
            //    _daemonThrd.Start();
            //}
        }

        /// <summary>
        /// 停止
        /// </summary>
        public void Stop()
        {
            //_isRunning = false;
            //if (_daemonThrd != null)
            //{
            //    if (_daemonThrd.IsAlive)
            //        _daemonThrd.Abort();
            //    _daemonThrd = null;
            //}

            Close();
        }

        // 关闭
        private void Close()
        {
            if (this._consumer != null)
            {
                try
                {
                    this._consumer.Close();
                }
                catch (Exception ex)
                {
                    LogHelper.WriteErrorLog(string.Format("关闭消费者异常:{0}", ex.Message));
                }
                this._consumer = null;
            }

            if (this._session != null)
            {
                try
                {
                    _session.Close();
                }
                catch (Exception ex)
                {
                    LogHelper.WriteErrorLog(string.Format("关闭消费者会话异常:{0}", ex.Message));
                }
                this._session = null;
            }

            if (this._connection != null)
            {
                try
                {
                    this._connection.Close();
                }
                catch (Exception ex)
                {
                    LogHelper.WriteErrorLog(string.Format("关闭消费者连接异常:{0}", ex.Message));
                }
                this._connection = null;
            }

            if (this._connectionFactory != null)
            {
                this._connectionFactory = null;
            }
        }


        /// <summary>
        /// 创建消费者普通订阅
        /// </summary>
        /// <returns></returns>
        private IMessageConsumer CreateConsumer()
        {
            IMessageConsumer consumer = null;
            try
            {
                // 连接工厂是用户创建连接的对象.
                if (_connectionFactory == null)
                {
                    Uri connectUri = new Uri("activemq:failover:(" + _brokerUrl + ")");
                    _connectionFactory = new NMSConnectionFactory(connectUri);
                }

                // 连接工厂创建一个连接
                this._connection = _connectionFactory.CreateConnection();
                if (this._connection != null)
                {
                    this._connection.ConnectionInterruptedListener += connection_ConnectionInterruptedListener;
                    if (!string.IsNullOrEmpty(this._clientID))
                        this._connection.ClientId = this._clientID;
                    // 启动连接
                    _connection.Start();
                    // 是生产和消费的一个单线程上下文。会话用于创建消息的生产者，消费者和消息。会话提供了一个事务性的上下文。
                    this._session = _connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                    // 创建消费者
                    if (_isPersistent && !string.IsNullOrEmpty(_clientID))
                    {
                        consumer = _session.CreateDurableConsumer(new Apache.NMS.ActiveMQ.Commands.ActiveMQTopic(this._topic), _clientID, null, false);
                    }
                    else
                    {
                        consumer = _session.CreateConsumer(new Apache.NMS.ActiveMQ.Commands.ActiveMQTopic(this._topic));
                    }
                    consumer.Listener += consumer_Listener;
                }
            }
            catch (Exception ex)
            {
                Close();
                LogHelper.WriteErrorLog(string.Format("创建消费者普通订阅异常:{0}", ex.Message));
            }
            return consumer;
        }

        // 连接中断监听
        void connection_ConnectionInterruptedListener()
        {
            Close();
            _consumer = CreateConsumer();
        }

        // 消息监听
        void consumer_Listener(IMessage message)
        {
            if (message is ITextMessage)
            {
                ITextMessage mqMessage = message as ITextMessage;
                if (mqMessage != null && _onMessageHandle != null)
                {
                    _onMessageHandle.BeginInvoke(mqMessage.Text, null, null);
                }
            }
            else if (message is IBytesMessage)
            {
                IBytesMessage mqMessage = message as IBytesMessage;
                if (mqMessage != null && _onMessageHandle != null)
                {
                    byte[] byteContent = new byte[1024];
                    int length = -1;
                    StringBuilder content = new StringBuilder();
                    while ((length = mqMessage.ReadBytes(byteContent)) != -1)
                    {
                        content.Append(Encoding.UTF8.GetString(byteContent, 0, length));
                    }
                    _onMessageHandle.BeginInvoke(content.ToString(), null, null);
                }
            }
        }

        #region 守护线程

        private bool _isRunning = false;

        private Thread _daemonThrd;

        private void DoWork()
        {
            while (_isRunning)
            {
                try
                {
                    // 创建消费者
                    if (_consumer == null)
                        _consumer = CreateConsumer();
                }
                catch (Exception ex)
                {
                    Close();
                    LogHelper.WriteErrorLog(string.Format("ActiveMQ守护线程异常:{0}", ex.Message));
                }

                Thread.Sleep(3000);
            }
        }

        #endregion
    }
}
