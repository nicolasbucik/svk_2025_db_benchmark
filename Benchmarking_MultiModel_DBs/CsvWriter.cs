namespace Benchmarking_MultiModel_DBs;

public class CsvWriter
{
    public static void WriteResultsToCsvWithResult(string filePath, List<BenchmarkResult> results)
    {
        using (var writer = new StreamWriter(filePath))
        {
            // Write CSV header
            writer.WriteLine("QueryName,ExecutionTimeMs,CacheType,QueryOutput");

            // Write each result
            foreach (var result in results)
            {
                writer.WriteLine($"{result.QueryName  ?? ""},{result.ExecutionTimeMs},{result.CacheType ?? ""},\"{result.QueryOutput ?? ""}\"");
            }
        }
    }
    
    public static void WriteResultsToCsvWithoutResult(string filePath, List<BenchmarkResult> results)
    {
        using (var writer = new StreamWriter(filePath))
        {
            // Write CSV header
            writer.WriteLine("QueryName,ExecutionTimeMs,CacheType");

            // Write each result
            foreach (var result in results)
            {
                writer.WriteLine($"{result.QueryName ?? ""},{result.ExecutionTimeMs},{result?.CacheType ?? ""}");
            }
        }
    }
}