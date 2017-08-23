using Apache.NMS;
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
    /// ActiveMQ生产者
    /// </summary>
    public class ActiveProducer
    {
        // 消息主题
        private string _topic;
        // 服务地址，如tcp://192.168.1.34:61616
        private string _brokerUrl;
        // 是否持久化
        private bool _isPersitent;

        // 消息生产者
        private IMessageProducer _producer = null;
        // 连接工厂
        private IConnectionFactory _connectionFactory = null;
        // 连接
        private IConnection _connection = null;
        // 会话 接受或者发送消息的线程
        private ISession _session = null;
        // 消息的目的地
        private IDestination _destination = null;


        /// <summary>
        /// 生产者构造函数
        /// </summary>
        /// <param name="topic">主题</param>
        /// <param name="brokerUrl">tcp://192.168.1.34:61616</param>
        public ActiveProducer(string topic, string brokerUrl)
        {
            this._topic = topic;
            this._brokerUrl = brokerUrl;
        }


        /// <summary>
        /// 生产者构造函数
        /// </summary>
        /// <param name="topic">主题</param>
        /// <param name="brokerUrl">服务地址，如tcp://192.168.1.34:61616</param>
        /// <param name="isPersitent">是否持久化</param>
        public ActiveProducer(string topic, string brokerUrl, bool isPersitent)
        {
            this._topic = topic;
            this._brokerUrl = brokerUrl;
            this._isPersitent = isPersitent;
            this._producer = CreateProducer();
        }

        /// <summary>
        /// 启动
        /// </summary>
        public void Start()
        {
            this._producer = CreateProducer();
            //_isRunning = true;
            //if (_daemonThrd == null || !_daemonThrd.IsAlive)
            //{
            //    _daemonThrd = new Thread(DoWork);
            //    _daemonThrd.IsBackground = true;
            //    _daemonThrd.Name = "activemq-producer-daemonThrd" + DateTime.Now.ToString("yyyyMMddHHssmmfff");
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

        /// <summary>
        /// 关闭连接和生产者
        /// </summary>
        private void Close()
        {

            if (this._producer != null)
            {
                try
                {
                    this._producer.Close();
                }
                catch (Exception) { }
                this._producer = null;
            }

            if (this._connection != null)
            {
                try
                {
                    this._connection.Close();
                }
                catch (Exception) { }
                this._connection = null;
            }

            if (this._session != null)
            {
                try
                {
                    this._session.Close();
                }
                catch (Exception) { }
                this._session = null;
            }
        }

        /// <summary>
        /// 创建生产者
        /// </summary>
        /// <returns></returns>
        private IMessageProducer CreateProducer()
        {
            IMessageProducer producer = null;
            try
            {
                // 连接工厂是用户创建连接的对象.
                if (_connectionFactory == null)
                {
                    Uri connectUri = new Uri("activemq:failover:(" + _brokerUrl + ")");
                    _connectionFactory = new NMSConnectionFactory(connectUri);
                }

                // 通过连接工厂获取连接
                this._connection = this._connectionFactory.CreateConnection();
                if (_connection != null)
                {
                    this._connection.ConnectionInterruptedListener += connection_ConnectionInterruptedListener;
                    // 启动连接
                    this._connection.Start();
                    // 创建_session
                    // this._session = this._connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                    this._session = this._connection.CreateSession();
                    // 消息的目的地
                    this._destination = SessionUtil.GetDestination(_session, "topic://" + this._topic);
                    // 创建消息生产者
                    producer = this._session.CreateProducer(this._destination);
                    // 设置是否持久化
                    producer.DeliveryMode = (_isPersitent ? MsgDeliveryMode.Persistent : MsgDeliveryMode.NonPersistent);
                }
            }
            catch (Exception ex)
            {
                Close();
                LogHelper.WriteErrorLog(string.Format("ActiveMQ创建生产者异常:{0}", ex.Message));
            }
            return producer;
        }

        // 连接中断监听
        void connection_ConnectionInterruptedListener()
        {
            Close();
            _producer = CreateProducer();
        }

        #region 发送消息

        /// <summary>
        /// 发送文本消息
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public bool SendTextMessage(string message)
        {
            bool bRet = false;
            try
            {
                ITextMessage writer = this._session.CreateTextMessage();
                writer.Text = message;
                this._producer.Send(writer);
                bRet = true;
            }
            catch (Exception ex)
            {
                LogHelper.WriteErrorLog(string.Format("ActiveMQ发送文本消息异常:{0}", ex.Message));
            }
            return bRet;
        }

        /// <summary>
        /// 批量发送文本消息
        /// </summary>
        /// <param name="msgList">文本消息列表</param>
        /// <returns></returns>
        public bool SendTextListMessage(List<string> msgList)
        {
            bool bRet = false;
            try
            {
                ITextMessage writer = this._session.CreateTextMessage();
                foreach (string msg in msgList)
                {
                    writer.Text = msg;
                    this._producer.Send(writer);
                }
                bRet = true;
            }
            catch (Exception ex)
            {
                LogHelper.WriteErrorLog(string.Format("ActiveMQ批量发送文本消息异常:{0}", ex.Message));
            }
            return bRet;
        }

        /// <summary>
        /// 发送字节消息
        /// </summary>
        /// <param name="message">字节消息</param>
        /// <returns></returns>
        public bool SendByteMessage(byte[] message)
        {
            bool bRet = false;
            try
            {
                IBytesMessage writer = this._session.CreateBytesMessage();
                writer.WriteBytes(message);
                this._producer.Send(writer);
                bRet = true;
            }
            catch (Exception ex)
            {
                LogHelper.WriteErrorLog(string.Format("ActiveMQ发送字节消息异常:{0}", ex.Message));
            }
            return bRet;
        }

        /// <summary>
        /// 批量发送字节消息
        /// </summary>
        /// <param name="msgList">字节消息列表</param>
        /// <returns></returns>
        public bool SendByteListMessage(List<byte[]> msgList)
        {
            bool bRet = false;
            try
            {
                IBytesMessage writer = this._session.CreateBytesMessage();
                foreach (byte[] msg in msgList)
                {
                    writer.WriteBytes(msg);
                    this._producer.Send(writer);
                }
                bRet = true;
            }
            catch (Exception ex)
            {
                LogHelper.WriteErrorLog(string.Format("ActiveMQ批量发送字节消息异常:{0}", ex.Message));
            }
            return bRet;
        }

        #endregion

        #region 守护线程

        private bool _isRunning = false;

        private Thread _daemonThrd;

        private void DoWork()
        {
            while (_isRunning)
            {
                try
                {
                    // 创建producer
                    if (_producer == null)
                        _producer = CreateProducer();

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
