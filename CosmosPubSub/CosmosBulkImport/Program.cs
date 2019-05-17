namespace CosmosBulkImport
{
    using System;  
    using System.Configuration;   
    using Microsoft.Azure.CosmosDB.BulkExecutor;
    using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
    using System.Threading.Tasks;
    using System.Threading;
    using System.Collections.Generic;
    using Newtonsoft.Json;
    using System.Diagnostics;
    using System.Net;
    using System.Linq;

    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;

    class Program
    {
        private static readonly string uri = ConfigurationManager.AppSettings["uri"];
        private static readonly string key = ConfigurationManager.AppSettings["key"];        
        private static readonly string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static readonly string CollectionName = ConfigurationManager.AppSettings["CollectionName"];
        private static readonly int DegreeOfParallelism = Int32.Parse(ConfigurationManager.AppSettings["DegreeOfParallelism"]);
        private static readonly int RUS = Int32.Parse(ConfigurationManager.AppSettings["RUS"]);
        private static readonly int NumberOfDocumentsToInsert = Int32.Parse(ConfigurationManager.AppSettings["NumberOfDocumentsToInsert"]);
        private static readonly string CollectionPartitionKey = ConfigurationManager.AppSettings["CollectionPartitionKey"];

        private static Database cosmosDatabase = null;

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
                        DocumentCollection collection = GetCollection(documentClient).GetAwaiter().GetResult();

                        //while(true)
                        //{
                            RunDemoAsync(documentClient, collection).GetAwaiter().GetResult();
                        //}                        
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
                    //Console.WriteLine($"Tried to insert: {NumberOfDocumentsToInsert} Documents.");
                    //Console.ReadKey();
                }
                               
        }

        private static async Task<DocumentCollection> GetCollection(DocumentClient client)
        {
            cosmosDatabase = await client.CreateDatabaseIfNotExistsAsync(new Database { Id = DatabaseName });
            return await GetOrCreateContainerAsync(cosmosDatabase, CollectionName, client);
        }

        private static async Task RunDemoAsync(DocumentClient client, DocumentCollection container)
        {                       
            await MassItemInsert(client, container);           
        }

        private static async Task<DocumentCollection> GetOrCreateContainerAsync(Database database, string containerId, DocumentClient client)
        {
            DocumentCollection dataCollection = GetCollectionIfExists(DatabaseName, CollectionName, client);

            if (dataCollection.ToString() != "")
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

        private static async Task MassItemInsert(DocumentClient client, DocumentCollection container)
        {

            // Combining bulk import and task list
            //List<List<Record>> BatchList = new List<List<Record>>();           

            //for (var i = 0; i < DegreeOfParallelism; i++)
            //{
            //BatchList.Add(await CreateItemsForBatch("Table" + i, NumberOfDocumentsToInsert / DegreeOfParallelism));
            //}

            List<Record> L = await CreateItemsForBatch("Table", NumberOfDocumentsToInsert / DegreeOfParallelism);
            while (true)
            {
                //var tasks = new List<Task>();
                IBulkExecutor bulkExecutor = new BulkExecutor(client, container);
                await bulkExecutor.InitializeAsync();

                Stopwatch s = new Stopwatch();
                s.Start();
                //for (var i = 0; i < DegreeOfParallelism; i++)
                //{
                var response = await bulkExecutor.BulkImportAsync(
                                    documents: L,
                                    enableUpsert: true,
                                    disableAutomaticIdGeneration: true,
                                    maxConcurrencyPerPartitionKeyRange: null,
                                    maxInMemorySortingBatchSize: null,
                                    cancellationToken: CancellationToken.None);
                //}
                Console.Write($"Tried to insert: {NumberOfDocumentsToInsert} Documents || ");
                Console.Write($"Number of Documents imported: {response.NumberOfDocumentsImported} || ");
                Console.Write($"Cosmos time: {response.TotalTimeTaken} || ");
                Console.Write($"Cosmos RUS: {response.TotalRequestUnitsConsumed} || ");


                /*for (var i = 0; i < DegreeOfParallelism; i++)
                {
                    tasks.Add(bulkExecutor.BulkImportAsync(
                            documents: BatchList[i],
                            disableAutomaticIdGeneration: true,
                                    maxConcurrencyPerPartitionKeyRange: null,
                                    maxInMemorySortingBatchSize: null
                        ));
                }*/


                //await Task.WhenAll(tasks);
                s.Stop();
                Console.WriteLine($"Elapsed Milliseconds (App): {s.ElapsedMilliseconds}");
            }
        }        

        public static async Task<List<Record>> CreateItemsForBatch(string TableName, int RecordsToCreate)
        {            
            List<Record> r = new List<Record>();

            for (int x = 0; x < DegreeOfParallelism; x++)
            {
                string tn = DegreeOfParallelism.ToString();
                for (int i = 0; i < RecordsToCreate; i++)
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

                        Field1 = new string('a', 100),
                        Field2 = new string('b', 100),
                        Field3 = new string('c', 100),
                        Field4 = new string('d', 100),
                        Field5 = new string('e', 100),

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
