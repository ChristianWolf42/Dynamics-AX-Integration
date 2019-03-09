using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace FigureThisOut
{
    class Program
    {
        //{"UniqueKeyList":[1,2,3,4,5,6,7,8,9,10}
        
        static void Main(string[] args)
        {
            string JSON = "{\"UniqueKeyList\":[1,2,3,4,5,6,7,8,9,10]}";

            Rootobject r = JsonConvert.DeserializeObject<Rootobject>(JSON);

            Console.WriteLine("bla");           
        }
    }
    public class Rootobject
    {
        public IList<int> UniqueKeyList { get; set; }
    }
}
