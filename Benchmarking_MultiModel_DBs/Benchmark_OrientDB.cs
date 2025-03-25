using System.Diagnostics;
using Orient.Client;

namespace Benchmarking_MultiModel_DBs
{
    public class Benchmark_OrientDB
    {
        private readonly Func<string> _queryBuilder;
        private readonly string _queryName;
        private readonly ODatabase _database;


        public Benchmark_OrientDB(Func<string> queryBuilder, string queryName, string hostname, int port, string databaseName, string userName, string userPassword, int poolSize, string alias)
        {
            _queryBuilder = queryBuilder;
            _queryName = queryName;

            // Create a pool of database connections
            OClient.CreateDatabasePool(hostname, port, databaseName, ODatabaseType.Graph, userName, userPassword, poolSize, alias);
            _database = new ODatabase(alias);
        }

        // Clear Cache function is not directly applicable in OrientDB, but you could manually clear the cache or run specific queries if needed.
        public async Task ClearCache()
        {
            // OrientDB does not have a built-in cache clear function like MongoDB.
            // You may need to rely on the database's configuration for cache management.
            Console.WriteLine("Clearing Cache (OrientDB-specific logic not implemented in this example)");
            await Task.CompletedTask;
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
            var warmCacheExecutionTimes = new List<long>();

            var warmCacheTasks = new List<Task<string>>();

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
                        QueryOutput = result.ToString()
                    });

                    return result;
                }));
            }
            var warmCacheResults = await Task.WhenAll(warmCacheTasks);

            Console.WriteLine($"{_queryName} - Warm Cache - Parallel Execution Time ({parallelClients} clients): {warmCacheExecutionTimes.Average()} ms");

            // Cold cache tests (clear cache before the first query)
            await ClearCache(); // Clear the cache before the first query
            var coldCacheExecutionTimes = new List<long>();
            var coldCacheTasks = new List<Task<string>>();

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
                        QueryOutput = result.ToString()
                    });

                    return result;
                }));
            }
            var coldCacheResults = await Task.WhenAll(coldCacheTasks);

            Console.WriteLine($"{_queryName} - Cold Cache - Parallel Execution Time ({parallelClients} clients): {coldCacheExecutionTimes.Average()} ms");

            return results;
        }

        // Execute a query and return the result
        private async Task<string> ExecuteQuery(string query)
        {
            // Execute SQL-like queries in OrientDB
            var result = _database.Query(query); // Assuming query returns documents
            
            return await Task.FromResult(result?.ToString() ?? "");
        }
    }
}
