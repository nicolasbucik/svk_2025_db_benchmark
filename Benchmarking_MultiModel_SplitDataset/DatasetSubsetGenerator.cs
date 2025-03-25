namespace Benchmarking_MultiModel_SplitDataset;

public class DatasetSubsetGenerator
{
    public static async Task GenerateRandomSubset(string inputFilePath, string outputFilePath, int maxLines)
    {
        var lines = new List<string?>();

        // Read all lines from the input file
        using (var reader = new StreamReader(inputFilePath))
        {
            string? line;
            while ((line = await reader.ReadLineAsync()) is not null)
            {
                lines.Add(line);
            }
        }
        
        if (lines.Count <= maxLines)
        {
            Console.WriteLine($"Dataset has only {lines.Count} entries. Using the full dataset.");
            maxLines = lines.Count;
        }
        
        var random = new Random(42);
        var shuffledLines = lines.OrderBy(x => random.Next()).ToList();

        // Write the first N lines to the output file
        using (var writer = new StreamWriter(outputFilePath))
        {
            for (int i = 0; i < maxLines; i++)
            {
                await writer.WriteLineAsync(shuffledLines[i]);
            }
        }
    }
}