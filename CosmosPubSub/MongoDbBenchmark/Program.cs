using MongoDB.Bson;
using MongoDB.Driver;
using System.Configuration;
using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using BenchmarkCommon;
using MongoDB.Bson.Serialization.Attributes;
using System.Linq;
using System.IO;
using System.Threading.Tasks;
using System.Diagnostics;

namespace MongoDbBenchmark
{
    public class Program
    {
        private static readonly string uri = ConfigurationManager.AppSettings["uri"];
        private static readonly string key = ConfigurationManager.AppSettings["key"];
        private static readonly string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static readonly string CollectionName = ConfigurationManager.AppSettings["CollectionName"];
        private static int[] NumberOfPartitions => StringUtils.ParseCsvIntValues(ConfigurationManager.AppSettings["NumberOfPartitions"]);
        private static readonly int NumberOfDocuments = Int32.Parse(ConfigurationManager.AppSettings["NumberOfDocuments"]);
        private static readonly int BatchSize = Int32.Parse(ConfigurationManager.AppSettings["BatchSize"]);
        private static readonly int StringFieldSize = Int32.Parse(ConfigurationManager.AppSettings["StringFieldSize"]);
        private static readonly int NumberOfTrials = Int32.Parse(ConfigurationManager.AppSettings["NumberOfTrials"]);
        private static readonly bool DeleteCollectionAfterUse = bool.Parse(ConfigurationManager.AppSettings["DeleteCollectionAfterUse"]);
        private static readonly string CollectionPartitionKey = ConfigurationManager.AppSettings["CollectionPartitionKey"];
        private static readonly string LogDestination = ConfigurationManager.AppSettings["LogDestination"];

        public static void Main(string[] args)
        {
            var client = new MongoClient(
                $"mongodb://13.77.166.155:27017"
            );

            MassItemInsert(client);
        }

        private static void MassItemInsert(MongoClient client)
        {
            File.WriteAllText(LogDestination, $"number_of_documents,number_of_partitions,elapsed_seconds\n");


            // Executes the experiment <NumberOfTrials> times for each pair <NumberOfPartitions, RUS>
            IMongoCollection<Record> collection = null;
            try
            {
                var database = client.GetDatabase(DatabaseName);
                collection = database.GetCollection<Record>(CollectionName);

                Console.WriteLine($"Collection {CollectionName} retrieved");

                for (var i = 0; i < NumberOfPartitions.Length; i++)
                {
                    int numberOfRecordsPerTable = NumberOfDocuments / NumberOfPartitions[i];

                    for (var j = 0; j < NumberOfTrials; j++)
                    {
                        var records = CreateItemsForBatch(NumberOfPartitions[i], numberOfRecordsPerTable);

                        var s = Stopwatch.StartNew();
                        var responses = BulkImport(collection, records);
                        s.Stop(); // TODO: how to get the actual elapsed time from the server?

                        File.AppendAllText(LogDestination, $"{NumberOfDocuments},{NumberOfPartitions[i]},{s.Elapsed.TotalSeconds}\n");

                        int totalDocuments = responses.Sum(r => r.ProcessedRequests.Count);

                        Console.Write($"Tried to insert: {NumberOfDocuments} Documents || ");
                        Console.Write($"Number of Documents imported: {totalDocuments} || ");
                        Console.Write($"Elapsed Milliseconds (client): {s.ElapsedMilliseconds}");
                        Console.Write($"Number of partitions: {NumberOfPartitions[i]} || ");
                        Console.Write($"Number of documents per partition: {numberOfRecordsPerTable} || ");
                        Console.Write($"Inserts per second: {totalDocuments / s.Elapsed.TotalSeconds} || ");
                    }
                }
            }
            finally
            {
                if (collection != null)
                {
                    //await client.DeleteDocumentCollectionAsync(container.DocumentsLink);
                    //Console.WriteLine($"Collection {container.DocumentsLink} deleted");
                }
            }
        }


        private static IList<BulkWriteResult<Record>> BulkImport(IMongoCollection<Record> collection, List<Record> records)
        {
            List<WriteModel<Record>> upsertOperations = new List<WriteModel<Record>>();

            foreach (Record record in records)
            {
                var filter = new FilterDefinitionBuilder<Record>().Where(m => m.Id == record.Id);
                ReplaceOneModel<Record> writeModel = new ReplaceOneModel<Record>(filter: filter, replacement: record);
                writeModel.IsUpsert = true;

                //InsertOneModel<Record> writeModel = new InsertOneModel<Record>(record);
                upsertOperations.Add(writeModel);

            }

            // Split operations in batches to be processed in parallel
            int batchSize = BatchSize;
            int i = 0;

            IEnumerable<IEnumerable<WriteModel<Record>>> batches = upsertOperations.GroupBy(key => i++ / batchSize).Select(x => x.Select(v => v).ToList()).ToList();

            var responses = new List<BulkWriteResult<Record>>();
            Parallel.ForEach(batches, new ParallelOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount*2},
                batch =>
            {
                BulkWriteResult<Record> response = collection.BulkWrite(batch, new BulkWriteOptions { IsOrdered = false });
                responses.Add(response);
            });

            
            return responses;
        }


        private static List<Record> CreateItemsForBatch(int numberOfTables, int numberOfRecordsPerTable)
        {
            List<Record> r = new List<Record>();

            for (int x = 1; x <= numberOfTables; x++)
            {
                string tn = $"Table-{x}";
                for (int i = 0; i < numberOfRecordsPerTable; i++)
                {
                    Record record = new Record
                    {
                        Id = ObjectId.GenerateNewId(),
                        OriginalId = Guid.NewGuid(),
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

        private sealed class Record
        {
            [BsonId]
            public ObjectId Id { get; set; }

            public Guid OriginalId { get; set; }

            public int RecId { get; set; }
            public string TableName { get; set; }
            public string Field1 { get; set; }
            public string Field2 { get; set; }
            public string Field3 { get; set; }
            public string Field4 { get; set; }
            public string Field5 { get; set; }

            public bool IsRegistered { get; set; }

            public DateTime RegistrationDate { get; set; }
        }
    }
}
