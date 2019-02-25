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
    class AddLinesThread
    {
        public String responseHdr { get; set; }
        public int LinesToCreate { get; set; }
        

        public void InsertLines()
        {
            int count = 0;

            HttpClient clientLocal = new HttpClient();
            clientLocal.BaseAddress = new Uri(ClientConfiguration.OneBox.UriString);
            clientLocal.DefaultRequestHeaders.Add(OAuthHelper.OAuthHeader, OAuthHelper.GetAuthenticationHeader());

            for (int i = 1; i <= LinesToCreate; i++)
            {
                string ServicePath = "/api/services/CFXServicegroup/CFXService/CreateLine";

                var mBody = new LineRecord
                {
                    _journalNum = responseHdr
                };

                HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, ServicePath);
                request.Content = new StringContent(JsonConvert.SerializeObject(mBody), Encoding.UTF8, "application/json");

                var result = clientLocal.SendAsync(request).Result;
                count++;
            }

            Console.Out.WriteLine(count.ToString());
        }
    }
}
