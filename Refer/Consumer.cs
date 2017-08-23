using Apache.NMS;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MQLX
{
    class Program
    {
        public static void Main(string[] args)
        {
            NMSConnectionFactory NMSFactory =
            new NMSConnectionFactory("tcp://localhost:61616");
            IConnection connection = NMSFactory.CreateConnection();
            ISession session =
            connection.
            CreateSession(AcknowledgementMode.AutoAcknowledge);
            IDestination destination =
            session.GetTopic("STOCKS.JAVA");
            IMessageConsumer consumer =
            session.CreateConsumer(destination);
            consumer.Listener += new MessageListener(OnMessage);
            connection.Start();

            Console.WriteLine("Press any key to quit.");
            Console.ReadKey();
        }
        protected static void OnMessage(IMessage message)
        {
            ITextMessage TextMessage = message as ITextMessage;
            Console.WriteLine(TextMessage.Text);
        }
    }
}

