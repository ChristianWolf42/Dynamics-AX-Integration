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

            BatchStatement batchStatement = new BatchStatement();            
            SimpleStatement statement = new SimpleStatement(configuration.GetSection("TestInsert").Value
                    .Replace("[keyspace]", configuration.GetSection("LoadTestCharacteristics").GetSection("CustomerKeySpace").Value.ToLower())
                    .Replace("[environment]", configuration.GetSection("LoadTestCharacteristics").GetSection("CustomerEnvironment").Value.ToLower()));
            
            for (int i = 0; i < 20; i++)
            {
                batchStatement.Add(statement);
            }

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            List<Task> list = new List<Task>();
            for (int i = 0; i < 500; i++)
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
                        $"{row.GetValue<Int64>("recid").ToString()} | " +
                        $"{row.GetValue<Guid>("uniqueid").ToString()} | " +
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
                Cluster cluster = Cluster.Builder()                    
                    .AddContactPoint(IPAddress.Parse(configuration.GetConnectionString("CassandraPublicIP")))
                    .WithPort(9042)
                    .WithAuthProvider(new PlainTextAuthProvider(configuration.GetConnectionString("DatabaseUser"), configuration.GetConnectionString("ClusterPassword")))
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
