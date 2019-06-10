namespace CassandraEval
{
    using System;
    using System.IO;
    using System.Net;
    using System.Threading.Tasks;   
    using System.Collections.Generic;
    using System.Diagnostics;
    using Microsoft.Extensions.Configuration;
    using Cassandra;
    class CassandraEvaluator
    {        
        static void Main(string[] args)
        {
            CassandraEvaluator cassandraEvaluator = new CassandraEvaluator();
            IConfigurationRoot configuration = cassandraEvaluator.BuildConfig().GetAwaiter().GetResult();
            ISession cassandraSession = cassandraEvaluator.initAsync(configuration).GetAwaiter().GetResult();

            if (cassandraSession != null)
            {
                if(cassandraEvaluator.TestInsertPerformance(cassandraSession, configuration).GetAwaiter().GetResult())
                {
                    Console.WriteLine("Performance Test completed");
                }
            }

            Console.ReadKey();
        }

        private async Task<bool> TestInsertPerformance(ISession cassandraSession, IConfigurationRoot configuration)
        {
            bool TestSuccess = false;                       

            // Setup Batch statements
            List<SimpleStatement> statementList = new List<SimpleStatement>();
            int numberOfDocuments = int.Parse(configuration.GetSection("LoadTestCharacteristics").GetSection("NumberOfDocuments").Value);
            for (int i = 0;i< numberOfDocuments; i++) // Number of documents
            {
                statementList.Add(new SimpleStatement(configuration.GetSection("VolumeInsert").Value
                    .Replace("[keyspace]", configuration.GetSection("LoadTestCharacteristics").GetSection("CustomerKeySpace").Value.ToLower())
                    .Replace("[environment]", configuration.GetSection("LoadTestCharacteristics").GetSection("CustomerEnvironment").Value.ToLower())
                    .Replace("[0]", i.ToString())
                    .Replace("[1]", i.ToString())));
            }

            // Build batch statements    
            List<BatchStatement> batchStatements = new List<BatchStatement>();
            int numberOfBatchStatements = int.Parse(configuration.GetSection("LoadTestCharacteristics").GetSection("NumberOfBatchStatements").Value);
            for (int i = 1; i < numberOfBatchStatements; i++) // Number of batch statements
            {
                BatchStatement batchStatement = new BatchStatement();
                for (int x = 1; x <= numberOfDocuments/numberOfBatchStatements; x++)// Number of threads and parallel executed batch statements
                {
                    batchStatement.Add(statementList[(x*i)-1]);
                }
                batchStatements.Add(batchStatement);
            }

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            List<Task> list = new List<Task>();
            foreach(BatchStatement batchStatement in batchStatements)
            {
                list.Add(cassandraSession.ExecuteAsync(batchStatement));
            }

            await Task.WhenAll(list);

            stopwatch.Stop();
            Console.WriteLine($"{stopwatch.ElapsedMilliseconds}");

            return TestSuccess;
        }

        private async Task<ISession> initAsync(IConfigurationRoot configuration)
        {
            ISession cassandraSession = null;

            if (configuration != null)
            {
                cassandraSession = await this.CreateCassandraSession(configuration);

                if (cassandraSession != null)
                {
                    Console.WriteLine($"Connected successfully to cluster: {cassandraSession.Cluster.Metadata.ClusterName}");
                    bool initSuccessful = await this.primeCassandraCluster(cassandraSession, configuration);

                    if(!initSuccessful)
                    {
                        Console.WriteLine("Cluster Initialization failed");
                    }
                }
            }

            return cassandraSession;
        }

        private async Task<bool> primeCassandraCluster(ISession cassandraSession, IConfigurationRoot configuration)
        {
            bool primingSuccessful = false;

            try
            {
                // Clean up key space before the next run
                SimpleStatement statement = new SimpleStatement(configuration.GetSection("DropKeySpace").Value
                    .Replace("[keyspace]", configuration.GetSection("LoadTestCharacteristics").GetSection("CustomerKeySpace").Value.ToLower()));
                await cassandraSession.ExecuteAsync(statement);

                statement = new SimpleStatement(configuration.GetSection("CreateKeySpace").Value
                    .Replace("[keyspace]", configuration.GetSection("LoadTestCharacteristics").GetSection("CustomerKeySpace").Value.ToLower()));
                await cassandraSession.ExecuteAsync(statement);

                // Create table if it doesn't exist
                statement = new SimpleStatement(configuration.GetSection("CreateTable").Value
                    .Replace("[keyspace]", configuration.GetSection("LoadTestCharacteristics").GetSection("CustomerKeySpace").Value.ToLower())
                    .Replace("[environment]", configuration.GetSection("LoadTestCharacteristics").GetSection("CustomerEnvironment").Value.ToLower()));
                await cassandraSession.ExecuteAsync(statement);

                // Test an insert statement
                statement = new SimpleStatement(configuration.GetSection("TestInsert").Value
                    .Replace("[keyspace]", configuration.GetSection("LoadTestCharacteristics").GetSection("CustomerKeySpace").Value.ToLower())
                    .Replace("[environment]", configuration.GetSection("LoadTestCharacteristics").GetSection("CustomerEnvironment").Value.ToLower()));
                await cassandraSession.ExecuteAsync(statement);

                // Test a select statement
                statement = new SimpleStatement(configuration.GetSection("TestSelect").Value
                    .Replace("[keyspace]", configuration.GetSection("LoadTestCharacteristics").GetSection("CustomerKeySpace").Value.ToLower())
                    .Replace("[environment]", configuration.GetSection("LoadTestCharacteristics").GetSection("CustomerEnvironment").Value.ToLower()));
                RowSet rowSet = await cassandraSession.ExecuteAsync(statement);

                // Fix this to be in the settings file as it is a pain to change here all the time if you change something in the settings file statements...
                foreach (var row in rowSet)
                {
                    Console.WriteLine($"{row.GetValue<string>("fnotablename")} | " +
                        $"{row.GetValue<Int64>("partitiontime").ToString()} | " +
                        $"{row.GetValue<System.SByte>("operation").ToString()} | " +
                        $"{row.GetValue<Guid>("transactionid").ToString()} | " +
                        $"{row.GetValue<Int64>("recid").ToString()} | " +                        
                        $"{row.GetValue<string>("document")}");
                }

                primingSuccessful = true;

            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }                        

            return primingSuccessful;
        }

        private async Task<IConfigurationRoot> BuildConfig()
        {
            IConfigurationRoot configuration = null;

            try
            {
                configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())                
                .AddJsonFile("appsettings.json")
                .AddJsonFile("CassandraQueries.json")
                .Build();
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            
            return configuration;
        }

        private async Task<ISession> CreateCassandraSession(IConfigurationRoot configuration)
        {
            ISession session = null;

            try
            {
                SocketOptions socketOptions = new SocketOptions();
                socketOptions.SetConnectTimeoutMillis(10000);

                Cluster cluster = Cluster.Builder()                    
                    .AddContactPoint(IPAddress.Parse(configuration.GetConnectionString("CassandraPublicIP")))
                    .WithPort(9042)
                    .WithAuthProvider(new PlainTextAuthProvider(configuration.GetConnectionString("DatabaseUser"), configuration.GetConnectionString("ClusterPassword")))
                    .WithSocketOptions(socketOptions)
                    .Build();

                session = cluster.Connect();
            }
            catch(NoHostAvailableException noHostAvailableException)
            {                
                Console.WriteLine(noHostAvailableException.ToString());
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

                
            return session;
        }

    }
}
