using AuthenticationUtility;
using Microsoft.OData.Client;
using ODataUtility.Microsoft.Dynamics.DataEntities;
using System;
using System.Threading.Tasks;
using System.Net.Http;
using System.Diagnostics;
using System.Threading;
using System.Net.Http.Headers;

namespace ODataSpeedTest
{
    class Program
    {
        public static string ODataEntityPath = ClientConfiguration.Default.UriString + "data";
        public static HttpClient client = new HttpClient();

        static void Main(string[] args)
        {
            // To test custom entities, regenerate "ODataClient.tt" file.
            // https://blogs.msdn.microsoft.com/odatateam/2014/03/11/tutorial-sample-how-to-use-odata-client-code-generator-to-generate-client-side-proxy-class/

            Uri oDataUri = new Uri(ODataEntityPath, UriKind.Absolute);
            var context = new Resources(oDataUri);

            context.SendingRequest2 += new EventHandler<SendingRequest2EventArgs>(delegate (object sender, SendingRequest2EventArgs e)
            {
                var authenticationHeader = OAuthHelper.GetAuthenticationHeader(useWebAppAuthentication: true);
                e.RequestMessage.SetHeader(OAuthHelper.OAuthHeader, authenticationHeader);
            });

            GetTopRecords(context).GetAwaiter().GetResult();

            // Uncomment below to run specific examples

            // 1. Simple query examples

            // QueryExamples.ReadLegalEntities(context);
            // QueryExamples.GetInlineQueryCount(context);
            // QueryExamples.GetTopRecords(context);
            // QueryExamples.FilterSyntax(context);
            // QueryExamples.FilterLinqSyntax(context);
            // QueryExamples.SortSyntax(context);
            // QueryExamples.FilterByCompany(context);
            // QueryExamples.ExpandNavigationalProperty(context);


            // 2. Simple CRUD examples

            // SimpleCRUDExamples.SimpleCRUD(context);

            // 2. Changeset examples

            // ODataChangesetsExample.CreateSalesOrderInSingleChangeset(context);
            // ODataChangesetsExample.CreateSalesOrderWithoutChangeset(context);
            /*
            Program P = new Program();
            var result = P.readRecords("/Data?$top=1000");
            */
            Console.ReadLine();
        }

        public static async Task GetTopRecords(Resources d365)
        {
            var vendorsQuery = d365.Vendors.AddQueryOption("$top", "10");
            var vendors = await vendorsQuery.ExecuteAsync() as QueryOperationResponse<Vendor>;

            foreach (var vendor in vendors)
            {
                Console.WriteLine("Vendor with ID {0} retrived.", vendor.VendorAccountNumber);
            }
        }

            /*
        public void DoAuth()
        {
            client.BaseAddress = new Uri(ClientConfiguration.OneBox.UriString);
            client.DefaultRequestHeaders.Add(OAuthHelper.OAuthHeader, OAuthHelper.GetAuthenticationHeader(true));
        }

        public string readRecords(string ServicePath)
        {
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, ODataEntityPath+ServicePath);

            var result = client.SendAsync(request).Result;

            HttpContent content = result.Content;
            content.Headers.ContentType = new MediaTypeWithQualityHeaderValue("application/json");
            var jsonResult = content.ReadAsStringAsync().Result;

            return jsonResult;
        }
        */
    }
}
