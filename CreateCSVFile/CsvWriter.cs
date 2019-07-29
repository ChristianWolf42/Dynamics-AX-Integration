using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace CreateCSVFile
{
    class CsvWriter : IFileWriter
    {
        async Task<bool> IFileWriter.WriteToFile(string path, long linesPerFile)
        {
            bool response = false;
            string fileName = path + "\\" + Guid.NewGuid().ToString() + ".csv";            

            try
            {
                var csv = new StringBuilder();
                csv.AppendLine(string.Format("id,varstring"));

                for (int i = 0; i < linesPerFile; i++)
                {
                    if(i == 0 || i % 1000 == 0)
                    {                                                
                        csv.AppendLine(string.Format("{0},{1}", i, new String('a', 1)));
                        await File.AppendAllTextAsync(fileName, csv.ToString());
                        csv.Clear();
                    }
                    else
                    {
                        csv.AppendLine(string.Format("{0},{1}", i, new String('a', 1)));
                    }
                    
                }
                
                await File.AppendAllTextAsync(fileName, csv.ToString());                
                
                response = true;
            }
            catch (Exception ex)
            {
                response = false;
                throw new Exception($"Failed to write csv file {ex.ToString()}");
            }            

            return response;
        }        
    }
}
