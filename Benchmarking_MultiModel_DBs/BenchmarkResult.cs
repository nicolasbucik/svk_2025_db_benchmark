namespace Benchmarking_MultiModel_DBs;

public class BenchmarkResult
{
    public string QueryName { get; set; }
    public long ExecutionTimeMs { get; set; }
    public string CacheType { get; set; }
    public string QueryOutput { get; set; }
}