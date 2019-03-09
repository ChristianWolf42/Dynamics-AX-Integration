using AuthenticationUtility;
using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Threading.Tasks;
using System.Net.Http;
using System.Diagnostics;
using System.Threading;
using System.Net.Http.Headers;

namespace JSONSvcSpeedTest
{
    class Program
    {
        public static HttpClient client = new HttpClient();
        static void Main(string[] args)
        {
            int linesToInsert = 1000;
            Stopwatch sw = new Stopwatch();

            Program P = new Program();

            P.DoAuth();

            sw.Start();
            P.SomeCall();
            sw.Stop();

            Console.WriteLine($"Overhead to create initial Session: {sw.ElapsedMilliseconds.ToString()}");
            /*
            sw.Reset();
            sw.Start();
            P.InsertLinesTable(linesToInsert);
            sw.Stop();

            Console.WriteLine($"Lines inserted to Table: {linesToInsert.ToString()}");
            Console.WriteLine(sw.ElapsedMilliseconds.ToString());

            sw.Reset();

            sw.Start();            
            P.InsertLinesEntity(linesToInsert);
            sw.Stop();
            
            Console.WriteLine($"Lines inserted to Entity: {linesToInsert.ToString()}");
            Console.WriteLine(sw.ElapsedMilliseconds.ToString());
            */

            sw.Reset();
            sw.Start();
            var result = P.readRecords("/api/services/SpeedTest/SpeedTestService/read1000RecordsEntity");
            sw.Stop();
            Rootobject deserialized = P.DeserializeResult(result);
          
            Console.WriteLine($"Read 1000 records from entity batch based: {sw.ElapsedMilliseconds.ToString()}");
            Console.ReadLine();
            Console.WriteLine(deserialized.UniqueKeyList[0]);            

            sw.Reset();
            sw.Start();
            result = P.readRecords("/api/services/SpeedTest/SpeedTestService/read1000RecordsTable");
            sw.Stop();
            deserialized = P.DeserializeResult(result);

            Console.WriteLine($"Read 1000 records from table batch based: {sw.ElapsedMilliseconds.ToString()}");
            Console.WriteLine(deserialized.UniqueKeyList[1]);
            Console.ReadLine();
        }

        private Rootobject DeserializeResult(string result)
        {
           char[] trim = { '"' };
           string res = result.Trim(trim);
           res = res.Replace("\\","");
           Rootobject r = JsonConvert.DeserializeObject<Rootobject>(res);

           return r;
        }

        public void DoAuth()
        {
            client.BaseAddress = new Uri(ClientConfiguration.OneBox.UriString);
            client.DefaultRequestHeaders.Add(OAuthHelper.OAuthHeader, OAuthHelper.GetAuthenticationHeader(true));
        }

        public string readRecords(string ServicePath)
        {
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, ServicePath);

            var result = client.SendAsync(request).Result;

            HttpContent content = result.Content;
            var jsonResult = content.ReadAsStringAsync().Result;

            return jsonResult;
        }


        public void SomeCall()
        {
            string ServicePath = "/api/services/SpeedTest/SpeedTestService/someCall";
            
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, ServicePath);                

            var result = client.SendAsync(request).Result;

            HttpContent content = result.Content;
            var jsonResult = content.ReadAsStringAsync().Result;            
        }

        public void InsertLinesEntity(int numberOfLines)
        {
            string ServicePath = "/api/services/SpeedTest/SpeedTestService/postEntity";

            for(int i = numberOfLines+1; i<=numberOfLines+numberOfLines; i++)
            {
                var contract = new Record
                {
                    _aStringField = "Entity insert via JSON",
                    _NaturalKey = i
                };

                HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, ServicePath);
                request.Content = new StringContent(JsonConvert.SerializeObject(contract), Encoding.UTF8, "application/json");

                var result = client.SendAsync(request).Result;

                HttpContent content = result.Content;
                var jsonResult = content.ReadAsStringAsync().Result;
            }            
        }

        public void InsertLinesTable(int numberOfLines)
        {
            string ServicePath = "/api/services/SpeedTest/SpeedTestService/postTable";

            for (int i = 1; i <= numberOfLines; i++)
            {
                var contract = new Record
                {
                    _aStringField = "Table insert via JSON",
                    _NaturalKey = i
                };

                HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, ServicePath);
                request.Content = new StringContent(JsonConvert.SerializeObject(contract), Encoding.UTF8, "application/json");

                var result = client.SendAsync(request).Result;

                HttpContent content = result.Content;
                var jsonResult = content.ReadAsStringAsync().Result;
            }
        }
    }
}
