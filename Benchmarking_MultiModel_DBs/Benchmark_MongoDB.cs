using System.Diagnostics;
using MongoDB.Bson;
using MongoDB.Driver;
using Renci.SshNet;

namespace Benchmarking_MultiModel_DBs;

public class Benchmark_MongoDB
{
    private readonly MongoDBBenchmark_QueryBuilder _mongoBenchmarkQueryBuilder;
    private readonly Func<string> _queryBuilder;
    private readonly string _queryName;
    private readonly IMongoDatabase _database;

    public Benchmark_MongoDB(MongoDBBenchmark_QueryBuilder mongoBenchmarkQueryBuilder, Func<string> queryBuilder, string queryName, string connectionString, string databaseName)
    {
        _mongoBenchmarkQueryBuilder = mongoBenchmarkQueryBuilder;
        _queryBuilder = queryBuilder;
        _queryName = queryName;
        var client = new MongoClient(connectionString);
        _database = client.GetDatabase(databaseName);
    }
    
    public async Task ClearCache()
    {
        var uniqueQuery = new BsonDocument { { "from", Guid.NewGuid().ToString() } };
        await _database.GetCollection<BsonDocument>("edges").Find(uniqueQuery).ToListAsync();
        var command2 = new BsonDocument { { "planCacheClear", "edges" } };
        await _database.RunCommandAsync<BsonDocument>(command2);
    }

    // Run non-parallel tests with warm and cold cache
    public async Task<List<BenchmarkResult>> RunNonParallelTests(int warmupRuns, int benchmarkRuns)
    {
        Console.WriteLine($"Running non-parallel tests for {_queryName}...");
        var results = new List<BenchmarkResult>();
        
        // Build the query (outside timing)
        var query = _queryBuilder();

        // Warmup runs (warm cache)
        for (int i = 0; i < warmupRuns; i++)
        {
            await ExecuteQuery(query);
        }

        // Benchmark runs (warm cache)
        var warmCacheExecutionTimes = new List<long>();
        for (int i = 0; i < benchmarkRuns; i++)
        {
            var stopwatch = Stopwatch.StartNew();
            var result = await ExecuteQuery(query);
            stopwatch.Stop();
            warmCacheExecutionTimes.Add(stopwatch.ElapsedMilliseconds);
            // Console.WriteLine($"{_queryName} - Warm Cache - Query Output: {result}");
            
            results.Add(new BenchmarkResult
            {
                QueryName = _queryName,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                CacheType = "Warm",
                QueryOutput = result.ToString()
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
            // Console.WriteLine($"{_queryName} - Cold Cache - Query Output: {result}");
            
            results.Add(new BenchmarkResult
            {
                QueryName = _queryName,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                CacheType = "Cold",
                QueryOutput = result.ToString()
            });
            
        }
        Console.WriteLine($"{_queryName} - Cold Cache - Average Execution Time: {coldCacheExecutionTimes.Average()} ms");
        
        return results;
    }

    // Run parallel tests with warm and cold cache
    public async Task<List<BenchmarkResult>> RunParallelTests(int warmupRuns, int benchmarkRuns, int parallelClients)
    {
        Console.WriteLine($"Running parallel tests for {_queryName}...");
        var results = new List<BenchmarkResult>();

        // Build the query (outside timing)
        var query = _queryBuilder();
        
        // Warmup runs (warm cache)
        for (int i = 0; i < warmupRuns; i++)
        {
            await ExecuteQuery(query);
        }

        // Warm cache tests
        var warmCacheTasks = new List<Task<BsonDocument>>();
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
                    CacheType = $"Warm (Parallel) Client: {i1+1}, Total Parallel Clients: {parallelClients}",
                    QueryOutput = result.ToString()
                });
                
                return result;
            }));
        }
        var warmCacheResults = await Task.WhenAll(warmCacheTasks);
        
        Console.WriteLine($"{_queryName} - Warm Cache - Parallel Execution Time ({parallelClients} clients): {(long)warmCacheExecutionTimes.Average()} ms");
        // Console.WriteLine($"{_queryName} - Warm Cache - Query Outputs:");
        foreach (var result in warmCacheResults)
        {
            // Console.WriteLine(result);
        }

        // Cold cache tests (clear cache before the first query)
        await ClearCache(); // Clear the cache before the first query
        var coldCacheExecutionTimes = new List<long>();
        var coldCacheTasks = new List<Task<BsonDocument>>();
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
                    CacheType = $"Cold (Parallel) Client: {i1+1}, Total Parallel Clients: {parallelClients}",
                    QueryOutput = result.ToString()
                });
                
                return result;
            }));
        }
        var coldCacheResults = await Task.WhenAll(coldCacheTasks);
        

        
        Console.WriteLine($"{_queryName} - Cold Cache - Parallel Execution Time ({parallelClients} clients): {(long)coldCacheExecutionTimes.Average()} ms");
        // Console.WriteLine($"{_queryName} - Cold Cache - Query Outputs:");
        foreach (var result in coldCacheResults)
        {
            // Console.WriteLine(result);
        }
        
        return results;
    }

    // Execute a query and return the result
    private async Task<BsonDocument> ExecuteQuery(string query)
    {
        return await _database.RunCommandAsync<BsonDocument>(query);
    }
}