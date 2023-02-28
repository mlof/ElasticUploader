using System.Collections.Concurrent;
using System.Globalization;
using CsvHelper;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using McMaster.Extensions.CommandLineUtils;

namespace ElasticUploader;

[HelpOption("-?|-h|--help")]
[Command(Description = "Uploads a csv file to elastic", Name = "elastic-upload", FullName = "Elastic Uploader",
        ExtendedHelpText = @"
A simple tool to upload a csv file to an Elasticsearch index.

If index name is not specified, the file name will be used.
Authentication can be done with either a cloud id and api key or a elastic uri and user/password.

Example usage:

elastic-upload -f C:\temp\test.csv -e http://localhost:9200 -u elastic -p changeme -b 1000

elastic-upload -f C:\temp\test.csv -c cloudid:cloudid -k apikey -b 1000


")]
public class Program
{
    [Option(Description = "File", ShortName = "f", LongName = "file")]
    public string File { get; }


    public static Task Main(string[] args)
    {
        return CommandLineApplication.ExecuteAsync<Program>();
    }

    private async Task OnExecuteAsync(CommandLineApplication app)
    {
        if (!HasRequiredOptions())
        {
            app.ShowHelp();
        }
        else
        {
            Console.WriteLine(" --- Starting upload --- ");

            if (string.IsNullOrWhiteSpace(IndexName))
            {
                Console.WriteLine("Index name not specified, using file name");
                IndexName = Path.GetFileNameWithoutExtension(File);
            }

            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine("Waiting 3 seconds before starting upload");


            await Task.Delay(3000);
            await Upload();
        }
    }

    private bool HasRequiredOptions()
    {
        if (string.IsNullOrWhiteSpace(File))
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(ElasticUri) || string.IsNullOrWhiteSpace(CloudId))
        {
            return false;
        }

        if ((string.IsNullOrWhiteSpace(ElasticUser) && string.IsNullOrWhiteSpace(ElasticPassword)) ||
            string.IsNullOrWhiteSpace(ApiKey))
        {
            return false;
        }

        return true;
    }

    private async Task Upload()
    {
        var header = GetAuthenticationHeader();


        var client = GetClient(header);


        var records = GetCsvRecords();


        await records.Buffer(this.BufferSize).ForEachAwaitAsync(async batch =>
        {
            var response = await client.BulkAsync(descriptor =>
                    descriptor.IndexMany(batch, (operationDescriptor, o) => operationDescriptor.Index(this.IndexName)));
            if (!response.IsValidResponse)
            {
                Console.WriteLine(response.DebugInformation);
            }
        });
    }

    private IAsyncEnumerable<dynamic> GetCsvRecords()
    {
        using var reader = new StreamReader(File);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);


        return csv.GetRecordsAsync<dynamic>();
    }

    [Option(Description = "Index name", ShortName = "i", LongName = "index")]
    public string IndexName { get; set; }

    private ElasticsearchClient GetClient(AuthorizationHeader header)
    {
        if (!string.IsNullOrWhiteSpace(CloudId))
        {
            return new ElasticsearchClient(this.CloudId, header);
        }

        if (!string.IsNullOrWhiteSpace(ElasticUri))
        {
            var settings = new ElasticsearchClientSettings(new Uri(ElasticUri))
                    .Authentication(header);

            return new ElasticsearchClient(settings);
        }

        throw new Exception("Either cloud id or elastic uri must be set");
    }

    private AuthorizationHeader GetAuthenticationHeader()
    {
        if (!string.IsNullOrWhiteSpace(this.ApiKey))
        {
            return new ApiKey(ApiKey);
        }

        if (!string.IsNullOrWhiteSpace(this.ElasticUser) && !string.IsNullOrWhiteSpace(this.ElasticPassword))
        {
            return new BasicAuthentication(ElasticUser, ElasticPassword);
        }

        throw new Exception("No valid authentication options");
    }

    [Option(Description = "Elastic uri", ShortName = "e", LongName = "elastic")]
    public string? ElasticUri { get; set; }

    [Option(Description = "Elastic user", ShortName = "u", LongName = "user")]
    public string? ElasticUser { get; set; }

    [Option(Description = "Elastic password", ShortName = "p", LongName = "password")]
    public string? ElasticPassword { get; set; }

    [Option(Description = "Buffer size", ShortName = "b", LongName = "buffer")]
    public int BufferSize { get; set; } = 1000;

    [Option(Description = "Api key", ShortName = "k", LongName = "key")]
    public string? ApiKey { get; set; }

    [Option(Description = "Cloud id", ShortName = "c", LongName = "cloud")]
    public string? CloudId { get; set; }
}