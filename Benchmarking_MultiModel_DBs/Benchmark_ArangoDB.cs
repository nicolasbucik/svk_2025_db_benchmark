using System.Diagnostics;
using ArangoDB.Client;
using ArangoDBNetStandard;
using Benchmarking_MultiModel_DBs;
using Renci.SshNet;

public class Benchmark_ArangoDB
{
    private readonly ArangoDBBenchmark_QueryBuilder _queryBuilder;
    private readonly Func<string> _query;
    private readonly string _queryName;
    private readonly ArangoDatabase _database;

    public Benchmark_ArangoDB(ArangoDBBenchmark_QueryBuilder queryBuilder, Func<string> query, string queryName, string connectionString, string databaseName)
    {
        _queryBuilder = queryBuilder;
        _query = query;
        _queryName = queryName;
        var settings = new DatabaseSharedSetting
        {
            Url = $"http://{connectionString}",
            Database = databaseName,         
            Credential = new System.Net.NetworkCredential("root", "admin")
        };
        _database = new ArangoDatabase(settings);
    }

    public async Task ClearCache()
    {
        using (var sshClient = new SshClient("", "root", ""))
        {
            try
            {
                sshClient.Connect();
        
                var command = sshClient.CreateCommand($"sudo systemctl restart arangodb3");
                command.Execute();
                sshClient.Disconnect();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error connecting to SSH: {ex.Message}");
            }
        }
        await Task.Delay(1000);
    }

    public async Task<List<BenchmarkResult>> RunNonParallelTests(int warmupRuns, int benchmarkRuns)
    {
        Console.WriteLine($"Running non-parallel tests for {_queryName}...");
        var results = new List<BenchmarkResult>();

        var query = _query();

        for (int i = 0; i < warmupRuns; i++)
        {
            await ExecuteQuery(query);
        }

        var warmCacheExecutionTimes = new List<long>();
        for (int i = 0; i < benchmarkRuns; i++)
        {
            var stopwatch = Stopwatch.StartNew();
            var result = await ExecuteQuery(query);
            stopwatch.Stop();
            warmCacheExecutionTimes.Add(stopwatch.ElapsedMilliseconds);

            results.Add(new BenchmarkResult
            {
                QueryName = _queryName,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                CacheType = "Warm",
                QueryOutput = string.Join(", ", result.Select(r => r.ToString()))
            });
        }
        Console.WriteLine($"{_queryName} - Warm Cache - Average Execution Time: {warmCacheExecutionTimes.Average()} ms");

        var coldCacheExecutionTimes = new List<long>();
        for (int i = 0; i < benchmarkRuns; i++)
        {
            await ClearCache();
            var stopwatch = Stopwatch.StartNew();
            var result = await ExecuteQuery(query);
            stopwatch.Stop();
            coldCacheExecutionTimes.Add(stopwatch.ElapsedMilliseconds);

            results.Add(new BenchmarkResult
            {
                QueryName = _queryName,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                CacheType = "Cold",
                QueryOutput = string.Join(", ", result.Select(r => r.ToString()))
            });
        }
        Console.WriteLine($"{_queryName} - Cold Cache - Average Execution Time: {coldCacheExecutionTimes.Average()} ms");

        return results;
    }

    public async Task<List<BenchmarkResult>> RunParallelTests(int warmupRuns, int benchmarkRuns, int parallelClients)
    {
        Console.WriteLine($"Running parallel tests for {_queryName}...");
        var results = new List<BenchmarkResult>();

        var query = _query();

        for (int i = 0; i < warmupRuns; i++)
        {
            await ExecuteQuery(query);
        }

        var warmCacheTasks = new List<Task<List<dynamic>>>();
        var warmCacheExecutionTimes = new List<long>();

        for (int i = 0; i < parallelClients; i++)
        {
            var i1 = i;
            warmCacheTasks.Add(Task.Run(async () =>
            {
                var stopwatch = Stopwatch.StartNew();
                var result = await ExecuteQuery(query);
                stopwatch.Stop();
                warmCacheExecutionTimes.Add(stopwatch.ElapsedMilliseconds);

                results.Add(new BenchmarkResult
                {
                    QueryName = _queryName,
                    ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                    CacheType = $"Warm (Parallel) Client: {i1 + 1}, Total Parallel Clients: {parallelClients}",
                    QueryOutput = Newtonsoft.Json.JsonConvert.SerializeObject(result, Newtonsoft.Json.Formatting.Indented)
                });

                return result;
            }));
        }
        var warmCacheResults = await Task.WhenAll(warmCacheTasks);

        Console.WriteLine($"{_queryName} - Warm Cache - Parallel Execution Time ({parallelClients} clients): {(long)warmCacheExecutionTimes.Average()} ms");

        await ClearCache();
        var coldCacheExecutionTimes = new List<long>();
        var coldCacheTasks = new List<Task<List<dynamic>>>();
        for (int i = 0; i < parallelClients; i++)
        {
            var i1 = i;
            coldCacheTasks.Add(Task.Run(async () =>
            {
                var stopwatch = Stopwatch.StartNew();
                var result = await ExecuteQuery(query);
                stopwatch.Stop();
                coldCacheExecutionTimes.Add(stopwatch.ElapsedMilliseconds);

                results.Add(new BenchmarkResult
                {
                    QueryName = _queryName,
                    ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                    CacheType = $"Cold (Parallel) Client: {i1 + 1}, Total Parallel Clients: {parallelClients}",
                    QueryOutput = Newtonsoft.Json.JsonConvert.SerializeObject(result, Newtonsoft.Json.Formatting.Indented)
                });

                return result;
            }));
        }
        var coldCacheResults = await Task.WhenAll(coldCacheTasks);

        Console.WriteLine($"{_queryName} - Cold Cache - Parallel Execution Time ({parallelClients} clients): {(long)coldCacheExecutionTimes.Average()} ms");

        return results;
    }
    
    private Task<List<dynamic>> ExecuteQuery(string query)
    {
        return Task.FromResult(_database.CreateStatement<dynamic>(query).ToList());
    }
}
