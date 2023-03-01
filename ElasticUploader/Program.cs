using System.Globalization;
using CsvHelper.Configuration;
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
    private CsvConfiguration csvConfiguration;

    [Option(Description = "FilePath", ShortName = "f", LongName = "file")]
    public string? FilePath { get; set; }


    [Option(Description = "Property name formatting", LongName = "property-formatting", ShortName = "pf")]
    public PropertyNameStrategy PropertyNameFormatting { get; set; } = PropertyNameStrategy.CamelCase;


    [Option(Description = "Index name", ShortName = "i", LongName = "index")]
    public string? IndexName { get; set; }

    [Option(Description = "Elastic uri", ShortName = "e", LongName = "elastic")]
    public string? ElasticUri { get; set; }

    [Option(Description = "Elastic user", ShortName = "u", LongName = "user")]
    public string? ElasticUser { get; set; }

    [Option(Description = "Elastic password", ShortName = "p", LongName = "password")]
    public string? ElasticPassword { get; set; }

    [Option(Description = "Buffer size", ShortName = "b", LongName = "buffer")]
    public int BufferSize { get; set; } = 1000;


    [Option(Description = "CSV Delimiter", ShortName = "d", LongName = "delimiter")]
    public string Delimiter { get; set; } = ",";

    [Option(Description = "Api Key", ShortName = "k", LongName = "key")]
    public string? ApiKey { get; set; }

    [Option(Description = "Cloud ID", ShortName = "c", LongName = "cloud")]
    public string? CloudId { get; set; }

    public static Task Main(string[] args)
    {
        return CommandLineApplication.ExecuteAsync<Program>(args);
    }

    // ReSharper disable once UnusedMember.Local
    private async Task OnExecuteAsync(CommandLineApplication app, CancellationToken cancellationToken = default)
    {
        if (!HasRequiredOptions()){
            app.ShowHelp();
        }
        else{
            Console.WriteLine(" --- Starting upload --- ");

            if (string.IsNullOrWhiteSpace(IndexName)){
                Console.WriteLine("Index name not specified, using file name");
                IndexName = Path.GetFileNameWithoutExtension(FilePath);
            }

            Console.WriteLine();
            Console.WriteLine();


            await Upload(cancellationToken);
        }
    }

    private bool HasRequiredOptions()
    {
        if (string.IsNullOrWhiteSpace(FilePath)){
            return false;
        }

        if (string.IsNullOrWhiteSpace(ElasticUri) && string.IsNullOrWhiteSpace(CloudId)){
            return false;
        }

        if (string.IsNullOrWhiteSpace(ElasticUser) && string.IsNullOrWhiteSpace(ElasticPassword) &&
            string.IsNullOrWhiteSpace(ApiKey)){
            return false;
        }

        return true;
    }

    private async Task Upload(CancellationToken cancellationToken = default)
    {
        var header = GetAuthenticationHeader();


        var client = GetClient(header);


        ArgumentException.ThrowIfNullOrEmpty(FilePath);
        await using var file = File.OpenRead(FilePath);
        using var reader = new CsvAsyncEnumerableReader(file, Delimiter, PropertyNameFormatting);
        var records = reader.GetRecords(cancellationToken);

        await records.Buffer(BufferSize).ForEachAwaitWithCancellationAsync(async (batch, index, token) =>
        {
            var response = await client.BulkAsync(descriptor =>
                    descriptor.IndexMany(batch, (operationDescriptor, o) => operationDescriptor.Index(IndexName)),
                token);
            if (response.IsValidResponse){
                Console.WriteLine(
                    $"{DateTime.Now.ToString("T", CultureInfo.CurrentCulture)}: \t Uploaded batch {index} of {batch.Count} records");
            }
            else if (!response.IsValidResponse){
                Console.WriteLine("Error uploading records");

                foreach (var item in response.ItemsWithErrors) Console.WriteLine(item.Error);
            }
        }, cancellationToken);

        Console.WriteLine(" --- Upload complete --- ");
    }

    private ElasticsearchClient GetClient(AuthorizationHeader header)
    {
        if (!string.IsNullOrWhiteSpace(CloudId)){
            return new ElasticsearchClient(CloudId, header);
        }

        if (!string.IsNullOrWhiteSpace(ElasticUri)){
            var settings = new ElasticsearchClientSettings(new Uri(ElasticUri))
                .Authentication(header);

            return new ElasticsearchClient(settings);
        }

        throw new Exception("Either cloud id or elastic uri must be set");
    }

    private AuthorizationHeader GetAuthenticationHeader()
    {
        if (!string.IsNullOrWhiteSpace(ApiKey)){
            var split = ApiKey.Split(':');
            return new Base64ApiKey(split[0], split[1]);
        }

        if (!string.IsNullOrWhiteSpace(ElasticUser) && !string.IsNullOrWhiteSpace(ElasticPassword)){
            return new BasicAuthentication(ElasticUser, ElasticPassword);
        }

        throw new Exception("No valid authentication options");
    }
}