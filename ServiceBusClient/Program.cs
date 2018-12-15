using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;

namespace ServiceBusClient
{
    internal class Program
    {
        #warning Add connection string here!
        private const string serviceBusConnectionString = "<CONNECTIONSTRING>";

        private const string queueName = "workflow";
        private static IQueueClient queueClient;

        public static void Main(string[] args)
        {
            mainAsync().GetAwaiter().GetResult();
        }

        private static async Task mainAsync()
        {
            queueClient = new QueueClient(serviceBusConnectionString, queueName, ReceiveMode.PeekLock)
            {
                PrefetchCount = 0
            };


            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after receiving all the messages.");
            Console.WriteLine("======================================================");

            // Register QueueClient's MessageHandler and receive messages in a loop
            registerOnMessageHandlerAndReceiveMessages();

            Console.ReadKey();

            await queueClient.CloseAsync();
        }

        private static void registerOnMessageHandlerAndReceiveMessages()
        {
            // Configure the MessageHandler Options in terms of exception handling, number of concurrent messages to deliver etc.
            MessageHandlerOptions messageHandlerOptions = new MessageHandlerOptions(exceptionReceivedHandler)
            {
                // Maximum number of Concurrent calls to the callback `ProcessMessagesAsync`, set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = 1,

                // Indicates whether MessagePump should automatically complete the messages after returning from User Callback.
                // False below indicates the Complete will be handled by the User Callback as in `ProcessMessagesAsync` below.
                AutoComplete = false,
                MaxAutoRenewDuration = TimeSpan.FromDays(30)
            };

            // Register the function that will process messages
            queueClient.RegisterMessageHandler(processMessagesAsync, messageHandlerOptions);
        }

        private static async Task processMessagesAsync(Message message, CancellationToken token)
        {
            // Process the message
            Console.Write(
                $"Locked until: {message.SystemProperties.LockedUntilUtc}. Remaining lock duration: {message.SystemProperties.LockedUntilUtc - DateTime.UtcNow}");

            await Task.Delay(10000);
            // Complete the message so that it is not received again.
            // This can be done only if the queueClient is created in ReceiveMode.PeekLock mode (which is default).
            await queueClient.CompleteAsync(message.SystemProperties.LockToken);

            Console.WriteLine(" - Complete!");

            // Note: Use the cancellationToken passed as necessary to determine if the queueClient has already been closed.
            // If queueClient has already been Closed, you may chose to not call CompleteAsync() or AbandonAsync() etc. calls 
            // to avoid unnecessary exceptions.
        }

        private static Task exceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            ExceptionReceivedContext context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }
    }
}