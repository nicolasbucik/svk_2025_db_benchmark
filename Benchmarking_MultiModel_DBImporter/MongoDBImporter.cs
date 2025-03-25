using MongoDB.Bson;
using MongoDB.Driver;

namespace Benchmarking_MultiModel_DBImporter;

public class MongoDBImporter
{
    private readonly IMongoDatabase _database;

    public MongoDBImporter(string connectionString, string databaseName)
    {
        var client = new MongoClient(connectionString);
        _database = client.GetDatabase(databaseName);
    }

    public async Task ImportEdges(string edgesFilePath)
    {
        var edgesCollection = _database.GetCollection<BsonDocument>("edges");

        using (var reader = new StreamReader(edgesFilePath))
        {
            string? line;
            while ((line = await reader.ReadLineAsync()) is not null)
            {
                var parts = line.Split(' ');
                if (parts.Length == 2)
                {
                    var document = new BsonDocument
                    {
                        { "from", parts[0] },
                        { "to", parts[1] }
                    };
                    await edgesCollection.InsertOneAsync(document);
                }
            }
        }
    }

    public async Task ImportFeatures(string featFilePath)
    {
        var featuresCollection = _database.GetCollection<BsonDocument>("features");

        using (var reader = new StreamReader(featFilePath))
        {
            string? line;
            while ((line = await reader.ReadLineAsync()) is not null)
            {
                var parts = line.Split(' ');
                if (parts.Length > 1)
                {
                    var nodeId = parts[0];
                    var features = parts.Skip(1).Select(int.Parse).ToList();

                    var document = new BsonDocument
                    {
                        { "nodeId", nodeId },
                        { "features", new BsonArray(features) }
                    };
                    await featuresCollection.InsertOneAsync(document);
                }
            }
        }
    }

    public async Task ImportCircles(string circlesFilePath)
    {
        var circlesCollection = _database.GetCollection<BsonDocument>("circles");

        using (var reader = new StreamReader(circlesFilePath))
        {
            string? line;
            while ((line = await reader.ReadLineAsync()) is not null)
            {
                var parts = line.Split(' ');
                if (parts.Length > 1)
                {
                    var circleName = parts[0];
                    var members = parts.Skip(1).ToList();

                    var document = new BsonDocument
                    {
                        { "circleName", circleName },
                        { "members", new BsonArray(members) }
                    };
                    await circlesCollection.InsertOneAsync(document);
                }
            }
        }
    }

    public async Task ImportEgoFeatures(string egofeatFilePath)
    {
        var egoFeaturesCollection = _database.GetCollection<BsonDocument>("egoFeatures");

        using (var reader = new StreamReader(egofeatFilePath))
        {
            var line = await reader.ReadLineAsync();
            if (line != null)
            {
                var features = line.Split(' ').Select(int.Parse).ToList();

                var document = new BsonDocument
                {
                    { "nodeId", "ego" }, // Ego node has a fixed ID
                    { "features", new BsonArray(features) }
                };
                await egoFeaturesCollection.InsertOneAsync(document);
            }
        }
    }

    public async Task ImportFeatureNames(string featnamesFilePath)
    {
        var featureNamesCollection = _database.GetCollection<BsonDocument>("featureNames");

        using (var reader = new StreamReader(featnamesFilePath))
        {
            string? line;
            int index = 0;
            while ((line = await reader.ReadLineAsync()) is not null)
            {
                var document = new BsonDocument
                {
                    { "index", index },
                    { "name", line }
                };
                await featureNamesCollection.InsertOneAsync(document);
                index++;
            }
        }
    }
}