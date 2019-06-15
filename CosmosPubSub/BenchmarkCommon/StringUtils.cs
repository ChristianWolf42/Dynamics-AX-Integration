using System;
using System.Linq;
using System.Text.RegularExpressions;

namespace BenchmarkCommon
{
    public static class StringUtils
    {
        public static int[] ParseCsvIntValues(string csvString)
        {
            if (csvString != null)
            {
                return Regex.Replace(csvString, @"\s+", "").Split(',').ToList().Select(token => int.Parse(token)).ToArray();
            }
            return new int[0];
        }
    }
}
