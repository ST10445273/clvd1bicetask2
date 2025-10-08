using Azure;
using Azure.Data.Tables;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure;
using System;
using System.Text.Json;

namespace QueueToTableFunction
{
    public class Function1
    {
        [FunctionName("QueueToTableFunction")]
        public void Run(
            [QueueTrigger("myqueue-items", Connection = "AzureWebJobsStorage")] string myQueueItem,
            ILogger log)
        {
            log.LogInformation($"C# Queue trigger function processed: {myQueueItem}");

            try
            {
                // Parse the queue message
                var messageData = JsonSerializer.Deserialize<JsonElement>(myQueueItem);

                // Get connection string from environment
                var connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");

                // Create table service client
                var tableServiceClient = new TableServiceClient(connectionString);
                var tableClient = tableServiceClient.GetTableClient("QueueMessages");

                // Ensure table exists
                tableClient.CreateIfNotExists();

                // Create entity to insert
                var entity = new TableEntity("queue-messages", Guid.NewGuid().ToString())
                {
                    {"MessageContent", myQueueItem},
                    {"InsertionTime", DateTime.UtcNow},
                    {"ProcessedTime", DateTime.UtcNow}
                };

                // Add custom properties from message if they exist
                if (messageData.ValueKind == JsonValueKind.Object)
                {
                    foreach (var property in messageData.EnumerateObject())
                    {
                        if (!entity.ContainsKey(property.Name))
                        {
                            entity[property.Name] = property.Value.ToString();
                        }
                    }
                }

                // Insert entity into table
                tableClient.AddEntity(entity);
                log.LogInformation($"Successfully inserted message with RowKey: {entity.RowKey}");
            }
            catch (JsonException ex)
            {
                log.LogError($"Error parsing JSON message: {ex.Message}");
            }
            catch (RequestFailedException ex)
            {
                log.LogError($"Table storage error: {ex.Message}");
            }
            catch (Exception ex)
            {
                log.LogError($"Error processing queue message: {ex.Message}");
                throw;
            }
        }
    }
}