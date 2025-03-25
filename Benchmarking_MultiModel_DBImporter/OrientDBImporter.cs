

using Orient.Client;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
namespace Benchmarking_MultiModel_DBImporter;
public class OrientDBImporter
{

    private readonly ODatabase _database;

    public OrientDBImporter(string hostname, int port, string databaseName, string userName, string userPassword, int poolSize, string alias)
    {

        // Create a pool of database connections
        OClient.CreateDatabasePool(hostname, port, databaseName, ODatabaseType.Graph, userName, userPassword, poolSize, alias);
        _database = new ODatabase(alias);
    }

     public async Task<bool> CollectionExistsAsync(string collectionName)
        {
            // In OrientDB, we check if a class exists
            return await Task.FromResult(_database.Class.Exists(collectionName));
        }

        public async Task<bool> DocumentExistsAsync(string collectionName, string documentKey)
        {
            try
            {
                // In OrientDB, we fetch the document by its key
                var document = _database.Load<ODocument>(collectionName, documentKey);
                return document != null;
            }
            catch
            {
                // If document is not found, it will throw an exception
                return false;
            }
        }

        public async Task ImportEdges(string edgesFilePath)
        {
            // Ensure the "users" class exists
            if (!await CollectionExistsAsync("users"))
            {
                _database.CreateClass("users", OClassType.Vertex);
            }

            // Ensure the "edges" class exists and is an edge class
            if (!await CollectionExistsAsync("edges"))
            {
                _database.CreateClass("edges", OClassType.Edge);
            }

            var edgesCollection = _database.Class.Get("edges");
            var usersCollection = _database.Class.Get("users");

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

                        // Insert users if they don't already exist
                        if (!await DocumentExistsAsync("users", fromUser))
                        {
                            var userDoc = new ODocument("users") { { "_key", fromUser } };
                            await Task.Run(() => _database.Save(userDoc));
                        }
                        if (!await DocumentExistsAsync("users", toUser))
                        {
                            var userDoc = new ODocument("users") { { "_key", toUser } };
                            await Task.Run(() => _database.Save(userDoc));
                        }

                        // Insert the edge
                        var edgeDoc = new ODocument("edges")
                        {
                            { "_from", $"users/{fromUser}" },
                            { "_to", $"users/{toUser}" }
                        };
                        await Task.Run(() => _database.Save(edgeDoc));
                    }
                }
            }
        }

        public async Task ImportFeatures(string featFilePath)
        {
            // Ensure the "features" class exists
            if (!await CollectionExistsAsync("features"))
            {
                _database.CreateClass("features", OClassType.Document);
            }

            var featuresCollection = _database.Class.Get("features");

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

                        var featureDoc = new ODocument("features")
                        {
                            { "_key", nodeId },
                            { "features", features }
                        };
                        await Task.Run(() => _database.Save(featureDoc));
                    }
                }
            }
        }

        public async Task ImportCircles(string circlesFilePath)
        {
            // Ensure the "circles" class exists
            if (!await CollectionExistsAsync("circles"))
            {
                _database.CreateClass("circles", OClassType.Document);
            }

            var circlesCollection = _database.Class.Get("circles");

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

                        var circleDoc = new ODocument("circles")
                        {
                            { "_key", circleName },
                            { "members", members }
                        };
                        await Task.Run(() => _database.Save(circleDoc));
                    }
                }
            }
        }

        public async Task ImportEgoFeatures(string egofeatFilePath)
        {
            // Ensure the "egoFeatures" class exists
            if (!await CollectionExistsAsync("egoFeatures"))
            {
                _database.CreateClass("egoFeatures", OClassType.Document);
            }

            var egoFeaturesCollection = _database.Class.Get("egoFeatures");

            using (var reader = new StreamReader(egofeatFilePath))
            {
                var line = await reader.ReadLineAsync();
                if (line != null)
                {
                    var features = line.Split(' ').Select(int.Parse).ToList();

                    var egoFeatureDoc = new ODocument("egoFeatures")
                    {
                        { "_key", "ego" }, // Ego node has a fixed ID
                        { "features", features }
                    };
                    await Task.Run(() => _database.Save(egoFeatureDoc));
                }
            }
        }

        public async Task ImportFeatureNames(string featnamesFilePath)
        {
            // Ensure the "featureNames" class exists
            if (!await CollectionExistsAsync("featureNames"))
            {
                _database.CreateClass("featureNames", OClassType.Document);
            }

            var featureNamesCollection = _database.Class.Get("featureNames");

            using (var reader = new StreamReader(featnamesFilePath))
            {
                string? line;
                int index = 0;
                while ((line = await reader.ReadLineAsync()) is not null)
                {
                    var featureNameDoc = new ODocument("featureNames")
                    {
                        { "_key", index.ToString() },
                        { "name", line }
                    };
                    await Task.Run(() => _database.Save(featureNameDoc));
                    index++;
                }
            }
        }
    }
}
