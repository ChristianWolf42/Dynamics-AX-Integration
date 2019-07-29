using System;
using Microsoft.Extensions.Configuration;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;

namespace CreateCSVFile
{
    class Program
    {
        static IConfigurationRoot configuration;

        static void Main(string[] args)
        {
            configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false)                
                .Build();

            //var applicationName = configuration["NumberOfCsvFilesToCreate"];
            Program program = new Program();
            program.Run(new CsvWriter()).GetAwaiter().GetResult();
            Console.ReadKey();
        }

        public async Task Run(IFileWriter fileWriter)
        {
            List<Task<bool>> filesToCreate = new List<Task<bool>>();

            int numberOfFiles;
            int numberOfLines;
            try
            {
                numberOfFiles = Convert.ToInt32(configuration["NumberOfCsvFilesToCreate"]);
                numberOfLines = Convert.ToInt32(configuration["NumberOfLinesPerFile"]);
            }
            catch(Exception ex)
            {
                throw new Exception($"Failed to convert to NumberOfCsvFilesToCreate or NumberOfLinesPerFile to Int32. Exception: {ex.ToString()}");
            }

            for (int i = 0; i < numberOfFiles; i++)
            {
                filesToCreate.Add(fileWriter.WriteToFile(path: configuration["FileLocation"], linesPerFile: numberOfLines));
            }

            var results = await Task.WhenAll(filesToCreate);

            var result = results.Where(r => r.Equals(false));

            if (result.Count() > 0)
                Console.WriteLine("Failed to Write all Files");
            else
                Console.WriteLine("Succeeded creating the CSV files");
        }
    }
}
