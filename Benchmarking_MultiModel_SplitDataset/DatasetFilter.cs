namespace Benchmarking_MultiModel_SplitDataset;

public class DatasetFilter
{
    public static async Task<HashSet<string>> ExtractUniqueNodes(string edgesFilePath)
    {
        var uniqueNodes = new HashSet<string>();

        using (var reader = new StreamReader(edgesFilePath))
        {
            string? line;
            while ((line = await reader.ReadLineAsync()) is not null)
            {
                var parts = line.Split(' ');
                if (parts.Length == 2)
                {
                    uniqueNodes.Add(parts[0]);
                    uniqueNodes.Add(parts[1]);
                }
            }
        }

        return uniqueNodes;
    }

    public static async Task FilterFileByNodes(string inputFilePath, string outputFilePath, HashSet<string> nodesToKeep)
    {
        using (var reader = new StreamReader(inputFilePath))
        using (var writer = new StreamWriter(outputFilePath))
        {
            string? line;
            while ((line = await reader.ReadLineAsync()) is not null)
            {
                var nodeId = line.Split(' ')[0];
                if (nodesToKeep.Contains(nodeId))
                {
                    await writer.WriteLineAsync(line);
                }
            }
        }
    }

    public static async Task<bool> ValidateNodeCoverage(string edgesFilePath, string featFilePath, string circlesFilePath)
    {
        var uniqueNodes = await ExtractUniqueNodes(edgesFilePath);
        
        using (var reader = new StreamReader(featFilePath))
        {
            string? line;
            while ((line = await reader.ReadLineAsync()) is not null)
            {
                var nodeId = line.Split(' ')[0];
                uniqueNodes.Remove(nodeId);
            }
        }
        
        using (var reader = new StreamReader(circlesFilePath))
        {
            string? line;
            while ((line = await reader.ReadLineAsync()) is not null)
            {
                var nodesInCircle = line.Split(' ').Skip(1).ToList();
                foreach (var nodeId in nodesInCircle)
                {
                    uniqueNodes.Remove(nodeId);
                }
            }
        }
        
        return uniqueNodes.Count == 0;
    }
}