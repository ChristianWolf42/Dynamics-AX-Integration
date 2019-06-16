using System;
using System.Data.SqlClient;
using System.Text;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Diagnostics;
using MoreLinq;

namespace sqltest
{
    class Program
    {
        public const Int32 count = 10000;
        public const Int32 NumberOfChunks = 20;
        public const Int32 batchSize = 500;
        public const Int32 parallelizationFactor = 2;
        public const Int32 iterations = 3;
        public static Int32[] stringLength = { 1, 10, 100, 200, 400 };

        static void Main(string[] args)
        {
            try
            {
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();

                builder.DataSource = "";
                builder.UserID = "";
                builder.Password = "";
                builder.InitialCatalog = "";
                builder.ConnectTimeout = 30000;

                for(int x = 0; x < stringLength.Length;x++)
                {
                    var testBatches = Program.prepareTestData(stringLength[x]).GetAwaiter().GetResult();
                    List<Int32> averages = new List<Int32>();

                    for (int i = 0; i < iterations; i++)
                    {
                        averages.Add(Program.BatchInserts(builder, testBatches, stringLength[x]).GetAwaiter().GetResult());
                    }

                    Console.WriteLine($"Average per second: {(averages.Sum()/iterations).ToString()} for nvarcharmax size: {stringLength[x]}\n");
                }
                                  

                // Validate output
                using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
                {
                    Console.WriteLine("\nTest Connection and Count + Query Last 15 rows:");
                    Console.WriteLine("=========================================\n");
                    
                    connection.Open();
                    
                    StringBuilder sb = new StringBuilder();
                    sb.Append("SELECT COUNT(*) FROM QueueTable;");
                                 
                    String sql = sb.ToString();

                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Console.WriteLine("{0}\n", reader.GetInt32(0).ToString());
                            }
                        }
                    }

                    sb = new StringBuilder();
                    sb.Append("SELECT TOP(15) * FROM QueueTable ORDER BY 3 DESC;");
                    sql = sb.ToString();

                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Console.WriteLine("{0} {1} {2} {3} {4} {5}", 
                                    reader.GetInt64(0).ToString(),
                                    reader.GetInt64(1).ToString(),
                                    reader.GetInt64(2).ToString(),
                                    reader.GetSqlByte(3).ToString(),
                                    reader.GetString(4).ToString(),
                                    reader.GetString(5).ToString());
                            }
                        }
                    }
                }
            }
            catch (SqlException e)
            {
                Console.WriteLine(e.ToString());
            }
            Console.WriteLine("\nPress enter to exit.");
            Console.ReadLine();
        }

        public static async Task<List<string>> prepareTestData(Int32 x)
        {
            List<String> insertsList = new List<string>();
            String nVarChar = new string('a', x);

            for (int i = 0; i < count; i++)
            {
                //insertsList.Add($"INSERT INTO QueueTable ( RecId, IsFullPush, TableName, FieldsAsJson ) VALUES ( 123, 1, N'SomeTableName', N'{nVarChar}')");
                insertsList.Add($"EXECUTE Inserter 123, 1, N'SomeTableName', N'{nVarChar}';  ");
            }

            //int cnt = 0;
            //IEnumerable<IEnumerable<string>> batches = insertsList.GroupBy(key => cnt++ / batchSize).Select(x => x.Select(v => v).ToList()).ToList();

            var batches = insertsList.Batch(batchSize);

            List<string> collapsedBatches = new List<string>();
            foreach (var batch in batches)
            {
                collapsedBatches.Add(string.Join(";", batch.ToArray()));
            }

            return collapsedBatches;
        }

        public static async Task<Int32> BatchInserts(SqlConnectionStringBuilder builder,List<string> collapsedBatches, Int32 nvarcharFieldSize)
        {            
            Stopwatch s = new Stopwatch();
            s.Start();
            
            Parallel.ForEach(collapsedBatches, new ParallelOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount * parallelizationFactor },
            batch =>
            {
                using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand("BEGIN TRAN;\n" + batch + "COMMIT TRAN;\n", connection))
                    //using (SqlCommand command = new SqlCommand(batch, connection))
                    {                                                
                        command.ExecuteNonQuery();                             
                    }
                }
            });
            
            /*
            var taskList = new List<Task>();
            foreach(var collapsedBatch in collapsedBatches)
            {
                taskList.Add(runBatch(builder, collapsedBatch));                
            }

            await Task.WhenAll(taskList);
            */

            s.Stop();
            Console.WriteLine($"Elapsed Milisecond: {s.ElapsedMilliseconds.ToString()}\n" +
                $"Inserts per second: {count/(s.ElapsedMilliseconds/1000)}\n" +
                $"Size of nvarchar(max) field: {nvarcharFieldSize.ToString()}\n");

            Int32 ret = (Int32)(count / (s.ElapsedMilliseconds / 1000));
            return ret;
        }

        /*
        public static async Task runBatch(SqlConnectionStringBuilder builder, string batch)
        {
            using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command = new SqlCommand(batch, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }
        */
    }
}

/*
   CREATE SEQUENCE PerfTestSequence
   AS bigint 
   START WITH 0 
   INCREMENT BY 1 
   NO CYCLE 
   CACHE 1000 ;

    CREATE TABLE QueueTable (
    SequenceID bigint PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR PerfTestSequence),
    RecId BigInt,
    TransactionId  bigint DEFAULT (CURRENT_TRANSACTION_ID()),
    IsFullPush TinyInt,
    TableName nvarchar(64),
    FieldsAsJson nvarchar(max)   
);

    sp_helpindex queuetable

    CREATE PROCEDURE Inserter   
    @RecId bigint,   
    @IsFullPush tinyint,
    @TableName nvarchar(64),
    @FieldsAsJson nvarchar(max)
AS
    INSERT INTO QueueTable 
    ( RecId, IsFullPush, TableName, FieldsAsJson ) 
    VALUES ( @RecId, @IsFullPush, @TableName, @FieldsAsJson)
GO
 */
