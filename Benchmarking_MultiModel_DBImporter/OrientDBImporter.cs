

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
        OClient.CreateDatabasePool(hostname, port, databaseName, ODatabaseType.Graph, userName, userPassword, poolSize, alias);
        _database = new ODatabase(alias);
    }

     public async Task<bool> CollectionExistsAsync(string collectionName)
        {
            return await Task.FromResult(_database.Class.Exists(collectionName));
        }

        public async Task<bool> DocumentExistsAsync(string collectionName, string documentKey)
        {
            try
            {
                var document = _database.Load<ODocument>(collectionName, documentKey);
                return document != null;
            }
            catch
            {
                return false;
            }
        }

        public async Task ImportEdges(string edgesFilePath)
        {
            if (!await CollectionExistsAsync("users"))
            {
                _database.CreateClass("users", OClassType.Vertex);
            }

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
                        { "_key", "ego" }, 
                        { "features", features }
                    };
                    await Task.Run(() => _database.Save(egoFeatureDoc));
                }
            }
        }

        public async Task ImportFeatureNames(string featnamesFilePath)
        {
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
