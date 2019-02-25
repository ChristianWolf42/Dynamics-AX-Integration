using AuthenticationUtility;
using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;
using System.Threading.Tasks;
using System.Net.Http;
using System.Diagnostics;
using System.Threading;

namespace CFXJSONLoadDriver
{
    class Program
    {
        public static HttpClient client = new HttpClient();        

        static void Main(string[] args)
        {
            Stopwatch sw = new Stopwatch();

            sw.Start();

            Program P = new Program();
            // 1 to N line journal
            int i = 1;

            P.DoAuth();
            String responseHdr = P.InsertHeader();
            P.InsertLine(responseHdr, i).GetAwaiter().GetResult();            

            sw.Stop();            

            Console.WriteLine(responseHdr);
            Console.WriteLine($"Lines inserted: {i.ToString()}");
            Console.WriteLine(sw.ElapsedMilliseconds.ToString());

            sw.Reset();
            sw.Start();
            P.Multithreading(200,100);
            sw.Stop();

            Console.WriteLine(sw.ElapsedMilliseconds.ToString());

            Console.ReadLine();
        }

        public void Multithreading(int threads, int linesNr)
        {            
            List<String> l = new List<String>();
            for (int i = 1; i <= threads; i++)
            {
                string responseHdr = InsertHeader();
                l.Add(responseHdr);
                Console.WriteLine(responseHdr);
            }

            foreach (string s in l)
            {
                AddLinesThread addLinesThread = new AddLinesThread();
                addLinesThread.LinesToCreate = linesNr;
                addLinesThread.responseHdr = s;
                Thread t = new Thread(new ThreadStart(addLinesThread.InsertLines));
                t.Start();
            }
        }

        public void DoAuth()
        {
            client.BaseAddress = new Uri(ClientConfiguration.OneBox.UriString);
            client.DefaultRequestHeaders.Add(OAuthHelper.OAuthHeader, OAuthHelper.GetAuthenticationHeader());
        }

        public String InsertHeader()
        {

            string ServicePath =  "/api/services/CFXServicegroup/CFXService/CreateHeader";                                                                   

            var mBody = new HeaderRecord
            {
                _journalName = "GLJ"
            };

            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, ServicePath);
            request.Content = new StringContent(JsonConvert.SerializeObject(mBody), Encoding.UTF8, "application/json");
            
            var result = client.SendAsync(request).Result;

            HttpContent content = result.Content;
            var jsonResult = content.ReadAsStringAsync().Result;

            return jsonResult;            
        }
        
        public async Task InsertLine(String JournalNum, int LinesToCreate)
        {
            //String jsonResult = "";

            for (int i = 1; i <= LinesToCreate; i++)
            {
                string ServicePath = "/api/services/CFXServicegroup/CFXService/CreateLine";

                var mBody = new LineRecord
                {
                    _journalNum = JournalNum
                };

                HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, ServicePath);
                request.Content = new StringContent(JsonConvert.SerializeObject(mBody), Encoding.UTF8, "application/json");

                var result = client.SendAsync(request).Result;

                //HttpContent content = result.Content;
                //jsonResult = content.ReadAsStringAsync().Result;                
            }

            //return jsonResult;
        }        
    }
}
