﻿namespace CosmosBulkImport
{
    using System;  
    using System.Configuration;   
    using Microsoft.Azure.CosmosDB.BulkExecutor;
    using System.Threading.Tasks;
    using System.Threading;
    using System.Collections.Generic;
    using Newtonsoft.Json;
    using System.Diagnostics;
    using System.Net;
    using System.Linq;

    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using System.IO;
    using System.Text.RegularExpressions;

    class Program
    {
        private static readonly string uri = ConfigurationManager.AppSettings["uri"];
        private static readonly string key = ConfigurationManager.AppSettings["key"];        
        private static readonly string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static readonly string CollectionName = ConfigurationManager.AppSettings["CollectionName"];
        private static int[] RUS = ParseCsvValue(ConfigurationManager.AppSettings["RUS"]);
        private static int[] NumberOfPartitions => ParseCsvValue(ConfigurationManager.AppSettings["NumberOfPartitions"]);
        private static readonly int NumberOfDocuments = Int32.Parse(ConfigurationManager.AppSettings["NumberOfDocuments"]);
        private static readonly int StringFieldSize = Int32.Parse(ConfigurationManager.AppSettings["StringFieldSize"]);
        private static readonly int NumberOfTrials = Int32.Parse(ConfigurationManager.AppSettings["NumberOfTrials"]);
        private static readonly bool DeleteCollectionAfterUse = bool.Parse(ConfigurationManager.AppSettings["DeleteCollectionAfterUse"]);
        private static readonly string CollectionPartitionKey = ConfigurationManager.AppSettings["CollectionPartitionKey"];
        private static readonly string LogDestination = ConfigurationManager.AppSettings["LogDestination"];

        private static Database cosmosDatabase = null;

        private static int[] ParseCsvValue(string csvValue)
        {
            if (csvValue != null)
            {
                return Regex.Replace(csvValue, @"\s+", "").Split(",").Select(token => int.Parse(token)).ToArray();
            }
            return new int[0];
        }

        public static void Main(string[] args)
        {
            ServicePointManager.DefaultConnectionLimit = int.MaxValue;
            
                try
                {
                    //Read the Cosmos endpointUrl and authorisationKeys from configuration
                    //These values are available from the Azure Management Portal on the Cosmos Account Blade under "Keys"
                    //NB > Keep these values in a safe & secure location. Together they provide Administrative access to your Cosmos account
                    DocumentClient documentClient = new DocumentClient(new Uri(uri), key, new ConnectionPolicy
                    {
                        ConnectionMode = ConnectionMode.Direct,
                        ConnectionProtocol = Protocol.Tcp,
                        MaxConnectionLimit = 50000
                    });

                    using (documentClient)
                    {

                        cosmosDatabase =  documentClient.CreateDatabaseIfNotExistsAsync(new Database { Id = DatabaseName }).GetAwaiter().GetResult();

                        

                        MassItemInsert(documentClient).GetAwaiter().GetResult();
                    }
                }
                catch (DocumentClientException cre)
                {
                    Console.WriteLine(cre.ToString());
                }
                catch (Exception e)
                {
                    Exception baseException = e.GetBaseException();
                    Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
                }
                finally
                {
                //Console.WriteLine($"Tried to insert: {NumberOfRecordsPerTable} Documents.");
                //Console.ReadKey();
            }

        }

        private static async Task<DocumentCollection> GetOrCreateContainerAsync(Database database, string containerId, DocumentClient client, int? RUS)
        {
            DocumentCollection dataCollection = GetCollectionIfExists(DatabaseName, CollectionName, client);

            if (dataCollection != null && dataCollection.ToString() != "")
            {
                return dataCollection;
            }
            else
            {
                DocumentCollection collectionDefinition = new DocumentCollection();
                collectionDefinition.Id = containerId;
                collectionDefinition.PartitionKey.Paths.Add(CollectionPartitionKey);
                collectionDefinition.IndexingPolicy.Automatic = false;

                return await client.CreateDocumentCollectionAsync(
                                UriFactory.CreateDatabaseUri(DatabaseName),
                                collectionDefinition,
                                new RequestOptions { OfferThroughput = RUS });
            }           
        }

        private static Database GetDatabaseIfExists(string databaseName, DocumentClient client)
        {
            return client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }

        private static DocumentCollection GetCollectionIfExists(string databaseName, string collectionName, DocumentClient client)
        {
            if (GetDatabaseIfExists(databaseName, client) == null)
            {
                return null;
            }

            return client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName))
                .Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
        }

        private static async Task MassItemInsert(DocumentClient client)
        {

            File.WriteAllText(LogDestination, $"number_of_documents,number_of_partitions,rus_provisioned,rus_consumed,elapsed_seconds\n");

            // Executes the experiment <NumberOfTrials> times for each pair <NumberOfPartitions, RUS>
            for (var i = 0; i < NumberOfPartitions.Length; i++)
            {
                for (var k = 0; k < RUS.Length; k++)
                {
                    int? rus = RUS[k];
                    DocumentCollection container = null;

                    try
                    {
                        container = await GetOrCreateContainerAsync(cosmosDatabase, CollectionName, client, rus);
                        Console.WriteLine($"Collection {container.DocumentsLink} with {rus} RUS created");

                        for (var j = 0; j < NumberOfTrials; j++)
                        {
                            int numberOfRecordsPerTable = NumberOfDocuments / NumberOfPartitions[i];
                            List<Record> L = CreateItemsForBatch(NumberOfPartitions[i], numberOfRecordsPerTable);

                            //var tasks = new List<Task>();
                            IBulkExecutor bulkExecutor = new BulkExecutor(client, container);
                            await bulkExecutor.InitializeAsync();

                            Stopwatch s = new Stopwatch();
                            s.Start();

                            var response = await bulkExecutor.BulkImportAsync(
                                                documents: L,
                                                enableUpsert: true,
                                                disableAutomaticIdGeneration: true,
                                                maxConcurrencyPerPartitionKeyRange: null,
                                                maxInMemorySortingBatchSize: null,
                                                cancellationToken: CancellationToken.None);
                            Console.Write($"Tried to insert: {NumberOfDocuments} Documents || ");
                            Console.Write($"Number of Documents imported: {response.NumberOfDocumentsImported} || ");
                            Console.Write($"Cosmos time: {response.TotalTimeTaken} || ");
                            Console.Write($"Cosmos RUS: {response.TotalRequestUnitsConsumed} || ");
                            Console.Write($"RUS provisioned: {rus} || ");
                            Console.Write($"Number of partitions: {NumberOfPartitions[i]} || ");
                            Console.Write($"Number of documents per partition: {numberOfRecordsPerTable} || ");
                            Console.Write($"Inserts per second: {response.NumberOfDocumentsImported / response.TotalTimeTaken.TotalSeconds} || ");

                            File.AppendAllText(LogDestination, $"{NumberOfDocuments},{NumberOfPartitions[i]},{rus},{response.TotalRequestUnitsConsumed},{response.TotalTimeTaken.TotalSeconds}\n");

                            s.Stop();
                            Console.WriteLine($"Elapsed Milliseconds (App): {s.ElapsedMilliseconds}");
                        }
                    }
                    finally
                    {
                        if (container != null)
                        {
                            await client.DeleteDocumentCollectionAsync(container.DocumentsLink);
                            Console.WriteLine($"Collection {container.DocumentsLink} deleted");
                        }
                    }
                }
            }
        }

        public static List<Record> CreateItemsForBatch(int numberOfTables, int numberOfRecordsPerTable)
        {
            List<Record> r = new List<Record>();

            for (int x = 1; x <= numberOfTables; x++)
            {
                string tn = $"Table-{x}";
                for (int i = 0; i < numberOfRecordsPerTable; i++)
                {
                    Record record = new Record
                    {
                        Id = System.Guid.NewGuid().ToString(),
                        TableName = tn,
                        RecId = i,

                        /*
                        Field1 = "a",
                        Field2 = "b",
                        Field3 = "c",
                        Field4 = "d",
                        Field5 = "e",
                        */

                        Field1 = new string('a', StringFieldSize),
                        Field2 = new string('b', StringFieldSize),
                        Field3 = new string('c', StringFieldSize),
                        Field4 = new string('d', StringFieldSize),
                        Field5 = new string('e', StringFieldSize),

                        /*
                        Field1 = new string('a', 1000),
                        Field2 = new string('b', 1000),
                        Field3 = new string('c', 1000),
                        Field4 = new string('d', 1000),
                        Field5 = new string('e', 1000),
                        */
                        /*
                        Field1 = new string('a', 5000),
                        Field2 = new string('b', 5000),
                        Field3 = new string('c', 5000),
                        Field4 = new string('d', 5000),
                        Field5 = new string('e', 5000),*/
                        IsRegistered = true,
                        RegistrationDate = DateTime.UtcNow.AddDays(-30)
                    };
                    r.Add(record);
                }
            }
            return r;
        }

        internal sealed class Record
        {
            [JsonProperty(PropertyName = "id")]
            public string Id { get; set; }

            public int RecId { get; set; }
            public string TableName { get; set; }
            public string Field1 { get; set; }
            public string Field2 { get; set; }
            public string Field3 { get; set; }
            public string Field4 { get; set; }
            public string Field5 { get; set; }

            public bool IsRegistered { get; set; }

            public DateTime RegistrationDate { get; set; }
            public string PartitionKey => TableName;

            public static string PartitionKeyPath => CollectionPartitionKey;
        }
    }
}
