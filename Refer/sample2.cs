class Program
         {
             static IConnectionFactory _factory = null;
             static IConnection _connection = null;
             static ITextMessage _message = null;
             static ITextMessage _message2 = null;
             static void Main(string[] args)
             {
                 //创建工厂
                _factory = new ConnectionFactory("tcp://localhost:61616/");
  11:              try
                {
                    //创建连接
                    using (_connection = _factory.CreateConnection())
                    {
                        //创建会话
                        using (ISession session = _connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
                        {
                            //创建一个主题
                           IDestination destination = new Apache.NMS.ActiveMQ.Commands.ActiveMQTopic("topic");
     
                            //创建生产者
                           IMessageProducer producer = session.CreateProducer(destination);
    
                            int counter = 0;
                            Console.WriteLine("请输入你要发送数据，然后回车！");
                            
                            while (true)
                            {
                                string msg = Console.ReadLine();
                                if (msg != string.Empty)
                                {
                                    Console.BackgroundColor = ConsoleColor.Blue;
                                    Console.Beep();
                                    counter++;
                                    
                                    //创建一个文本消息
                                    _message = producer.CreateTextMessage("This is .Net AcitveMQ Message....");
                                    //_message2 = producer.CreateTextMessage("<Root><First>Test</First><Root>");
                                    
                                    Console.WriteLine("正在发送第{0}组发送数据...........", counter);
                                    //发送消息
                                    producer.Send(_message, MsgDeliveryMode.Persistent, MsgPriority.Normal,
                                       TimeSpan.MaxValue);
     
                                    Console.WriteLine("'"+_message.Text+"'已经发送！");
                                   //producer.Send(_message2, MsgDeliveryMode.Persistent, MsgPriority.Normal,
                                    //    TimeSpan.MinValue);
                                    //Console.WriteLine("'" + _message2.Text + "'已经发送！");
                                    producer.Send(msg, MsgDeliveryMode.Persistent, MsgPriority.Normal, TimeSpan.MinValue);
                                    Console.WriteLine("你输入的数据'" + msg + "'已经发送！");
                                    Console.WriteLine("**************************************************");
                                   Console.BackgroundColor = ConsoleColor.Black;
                                    Console.WriteLine("请输入你要发送数据，然后回车！");                                
                                }
                            }                     
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
                Console.ReadLine();
            }
        }
