namespace CosmosLoadDriver
{
    using System;
    using System.Configuration;
    using Microsoft.Azure.Cosmos;
    using System.Threading.Tasks;
    using System.Threading;
    using System.Collections.Generic;
    using Newtonsoft.Json;
    using System.Diagnostics;
    using System.Net;

    class Program
    {
        private static readonly string ConnectionString = ConfigurationManager.AppSettings["ConnectionString"];
        private static readonly string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static readonly string CollectionName = ConfigurationManager.AppSettings["CollectionName"];
        private static readonly int DegreeOfParallelism = Int32.Parse(ConfigurationManager.AppSettings["DegreeOfParallelism"]);
        private static readonly int RUS = Int32.Parse(ConfigurationManager.AppSettings["RUS"]);
        private static readonly int NumberOfDocumentsToInsert = Int32.Parse(ConfigurationManager.AppSettings["NumberOfDocumentsToInsert"]);
        private static readonly string CollectionPartitionKey = ConfigurationManager.AppSettings["CollectionPartitionKey"];
        
        private static CosmosDatabase cosmosDatabase = null;

        // public static void Main(string[] args)
        // {
            //Task.WaitAll(Task.Run(() => Maina(args)), Task.Run(() => Maina(args)));
        // }

        public static void Main(string[] args)
        {
            ServicePointManager.DefaultConnectionLimit = int.MaxValue;

            try
            {
                //Read the Cosmos endpointUrl and authorisationKeys from configuration
                //These values are available from the Azure Management Portal on the Cosmos Account Blade under "Keys"
                //NB > Keep these values in a safe & secure location. Together they provide Administrative access to your Cosmos account
                CosmosClientBuilder clientBuilder = new CosmosClientBuilder(ConnectionString);
                clientBuilder.UseConnectionModeDirect();                
               
                using (CosmosClient client = clientBuilder.Build())
                {
                    CosmosContainer container = Program.createDB(client).GetAwaiter().GetResult();

                   //while(true)
                   //{
                        Program.RunDemoAsync(container).GetAwaiter().GetResult();
                   //}                        
                }
            }
            catch (CosmosException cre)
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
                Console.WriteLine($"{NumberOfDocumentsToInsert} Documents have been inserted.");
                Console.ReadKey();
            }
        }

        private static async Task<CosmosContainer> createDB(CosmosClient client)
        {
            cosmosDatabase = await client.Databases.CreateDatabaseIfNotExistsAsync(DatabaseName);
            return await Program.GetOrCreateContainerAsync(cosmosDatabase, CollectionName);
        }

        private static async Task RunDemoAsync(CosmosContainer container)
        {            
            await Program.MassItemInsert(container);            
        }

        private static async Task<CosmosContainer> GetOrCreateContainerAsync(CosmosDatabase database, string containerId)
        {
            CosmosContainerSettings containerDefinition = new CosmosContainerSettings(id: containerId, partitionKeyPath: CollectionPartitionKey);
            containerDefinition.IndexingPolicy = new IndexingPolicy();
            containerDefinition.IndexingPolicy.Automatic = false;
            containerDefinition.IndexingPolicy.IndexingMode = IndexingMode.None;

            return await database.Containers.CreateContainerIfNotExistsAsync(
                containerSettings: containerDefinition,
                throughput: RUS);
        }
        
        private static async Task MassItemInsert(CosmosContainer container)
        {
            
            var tasks = new List<Task>();

            Stopwatch s = new Stopwatch();
            s.Start();
            for (var i=0; i < DegreeOfParallelism; i++)
            {
                tasks.Add(CreateItems(container, "Table"+i, NumberOfDocumentsToInsert/DegreeOfParallelism));
            }
            
            await Task.WhenAll(tasks);
            s.Stop();
            Console.WriteLine("Elapsed Milliseconds: {0}", s.ElapsedMilliseconds);

            /*
            Thread[] t = new Thread[DegreeOfParallelism];

            for (var i = 0; i < DegreeOfParallelism; i++)
            {
                t[i] = new Thread(() => CreateItems(container, "Table" + i, NumberOfDocumentsToInsert / DegreeOfParallelism));
                t[i].Start();
            }

            for (int i = 0; i < t.Length; i++)
                t[i].Join();
            */
        }
        public static async Task CreateItems(CosmosContainer container,string TableName,int RecordsToCreate)
        {
            for(int i = 0;i < RecordsToCreate;i++)
            {
                Record record = new Record
                {
                    Id = System.Guid.NewGuid().ToString(),
                    RecId = i,
                    TableName = TableName,
                    Field1 = "bla",
                    Field2 = "blub",
                    Field3 = "blooming",
                    Field4 = "blossom",
                    Field5 = "blib",
                    IsRegistered = true,
                    RegistrationDate = DateTime.UtcNow.AddDays(-30)
                };

                //await container.Items.UpsertItemAsync<Record>(record.PartitionKey, record);
               await container.Items.CreateItemAsync<Record>(record.PartitionKey, record);               
                //await Task.Delay(10);
            }            
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
            public string PartitionKey => Id;

            public static string PartitionKeyPath => CollectionPartitionKey;
        }
       
    }
}
