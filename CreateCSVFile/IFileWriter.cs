using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CreateCSVFile
{
    interface IFileWriter
    {
        Task<bool> WriteToFile(string path, long linesPerFile);
    }
}
