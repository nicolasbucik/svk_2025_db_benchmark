namespace Benchmarking_MultiModel_DBImporter;
using ArangoDB.Client;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

public class ArangoDBImporter
{
    private readonly ArangoDatabase _database;

    public ArangoDBImporter(string connectionString, string databaseName)
    {
        var settings = new DatabaseSharedSetting
        {
            Url = $"http://{connectionString}",
            Database = databaseName,
            Credential = new System.Net.NetworkCredential("root", "admin")
        };

        _database = new ArangoDatabase(settings);
    }

    public async Task<bool> CollectionExistsAsync(string collectionName)
    {
        var collections = await _database.ListCollectionsAsync();
        return collections.Any(c => c.Name == collectionName);
    }

    public async Task<bool> DocumentExistsAsync(string collectionName, string documentKey)
    {
        try
        {
            var document = await _database.Collection(collectionName).DocumentAsync<object>(documentKey);
            return document != null;
        }
        catch (ArangoServerException)
        {
            return false;
        }
    }

    public async Task ImportEdges(string edgesFilePath)
    {
        if (!await CollectionExistsAsync("users"))
        {
            await _database.CreateCollectionAsync("users", type: CollectionType.Document);
        }

        if (!await CollectionExistsAsync("edges"))
        {
            await _database.CreateCollectionAsync("edges", type: CollectionType.Edge);
        }

        var edgesCollection = _database.Collection("edges");
        var usersCollection = _database.Collection("users");

        using (var reader = new StreamReader(edgesFilePath))
        {
            string? line;
            while ((line = await reader.ReadLineAsync()) is not null)
            {
                var parts = line.Split(' ');
                if (parts.Length == 2)
                {
                    var fromUser = parts[0];
                    var toUser = parts[1];

                    if (!await DocumentExistsAsync("users", fromUser))
                    {
                        await usersCollection.InsertAsync(new { _key = fromUser });
                    }
                    if (!await DocumentExistsAsync("users", toUser))
                    {
                        await usersCollection.InsertAsync(new { _key = toUser });
                    }

                    // Insert the edge
                    var document = new Dictionary<string, object>
                    {
                        { "_from", $"users/{fromUser}" },
                        { "_to", $"users/{toUser}" }
                    };
                    await edgesCollection.InsertAsync(document);
                }
            }
        }
    }

    public async Task ImportFeatures(string featFilePath)
    {
        if (!await CollectionExistsAsync("features"))
        {
            await _database.CreateCollectionAsync("features", type: CollectionType.Document);
        }

        var featuresCollection = _database.Collection("features");

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

                    var document = new Dictionary<string, object>
                    {
                        { "_key", nodeId },
                        { "features", features }
                    };
                    await featuresCollection.InsertAsync(document);
                }
            }
        }
    }

    public async Task ImportCircles(string circlesFilePath)
    {
        if (!await CollectionExistsAsync("circles"))
        {
            await _database.CreateCollectionAsync("circles", type: CollectionType.Document);
        }

        var circlesCollection = _database.Collection("circles");

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

                    var document = new Dictionary<string, object>
                    {
                        { "_key", circleName },
                        { "members", members }
                    };
                    await circlesCollection.InsertAsync(document);
                }
            }
        }
    }

    public async Task ImportEgoFeatures(string egofeatFilePath)
    {
        if (!await CollectionExistsAsync("egoFeatures"))
        {
            await _database.CreateCollectionAsync("egoFeatures", type: CollectionType.Document);
        }

        var egoFeaturesCollection = _database.Collection("egoFeatures");

        using (var reader = new StreamReader(egofeatFilePath))
        {
            var line = await reader.ReadLineAsync();
            if (line != null)
            {
                var features = line.Split(' ').Select(int.Parse).ToList();

                var document = new Dictionary<string, object>
                {
                    { "_key", "ego" }, // Ego node has a fixed ID
                    { "features", features }
                };
                await egoFeaturesCollection.InsertAsync(document);
            }
        }
    }

    public async Task ImportFeatureNames(string featnamesFilePath)
    {
        if (!await CollectionExistsAsync("featureNames"))
        {
            await _database.CreateCollectionAsync("featureNames", type: CollectionType.Document);
        }

        var featureNamesCollection = _database.Collection("featureNames");

        using (var reader = new StreamReader(featnamesFilePath))
        {
            string? line;
            int index = 0;
            while ((line = await reader.ReadLineAsync()) is not null)
            {
                var document = new Dictionary<string, object>
                {
                    { "_key", index.ToString() },
                    { "name", line }
                };
                await featureNamesCollection.InsertAsync(document);
                index++;
            }
        }
    }
}
