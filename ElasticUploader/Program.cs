using System.Globalization;
using CsvHelper;
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

    [Option(Description = "File", ShortName = "f", LongName = "file")]
    public string File { get; }


    [Option(Description = "Property name formatting", LongName = "property-formatting", ShortName = "pf")]
    public PropertyNameStrategy PropertyNameFormatting { get; set; } = PropertyNameStrategy.Default;

    public static Task Main(string[] args)
    {
        return CommandLineApplication.ExecuteAsync<Program>(args);
    }

    private async Task OnExecuteAsync(CommandLineApplication app, CancellationToken cancellationToken = default)
    {
        if (!HasRequiredOptions())
        {
            app.ShowHelp();
        }
        else
        {
            this.csvConfiguration = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                    HasHeaderRecord = true,
                    Delimiter = this.Delimiter,
                    IgnoreBlankLines = true,
                    TrimOptions = TrimOptions.Trim
            };

            csvConfiguration.PrepareHeaderForMatch = PropertyNameFormatting switch
            {
                    PropertyNameStrategy.Default => args =>
                            System.Text.Json.JsonNamingPolicy.CamelCase.ConvertName(args.Header),
                    PropertyNameStrategy.CamelCase => args =>
                            System.Text.Json.JsonNamingPolicy.CamelCase.ConvertName(args.Header),
                    PropertyNameStrategy.Lower => args => args.Header.ToLower(CultureInfo.InvariantCulture),
                    PropertyNameStrategy.Upper => args => args.Header.ToUpper(CultureInfo.InvariantCulture),
                    _ => throw new ArgumentOutOfRangeException()
            };

            csvConfiguration.PrepareHeaderForMatch = args => args.Header.ToLower();
            Console.WriteLine(" --- Starting upload --- ");

            if (string.IsNullOrWhiteSpace(IndexName))
            {
                Console.WriteLine("Index name not specified, using file name");
                IndexName = Path.GetFileNameWithoutExtension(File);
            }

            Console.WriteLine();
            Console.WriteLine();


            await Upload();
        }
    }

    private bool HasRequiredOptions()
    {
        if (string.IsNullOrWhiteSpace(File))
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(ElasticUri) && string.IsNullOrWhiteSpace(CloudId))
        {
            return false;
        }

        if ((string.IsNullOrWhiteSpace(ElasticUser) && string.IsNullOrWhiteSpace(ElasticPassword)) &&
            string.IsNullOrWhiteSpace(ApiKey))
        {
            return false;
        }

        return true;
    }

    private async Task Upload(CancellationToken cancellationToken = default)
    {
        var header = GetAuthenticationHeader();


        var client = GetClient(header);


        using var reader = new StreamReader(File);
        using var csv = new CsvReader(reader, this.csvConfiguration);


        var records = csv.GetRecordsAsync<dynamic>(cancellationToken);

        await records.Buffer(this.BufferSize).ForEachAwaitWithCancellationAsync(async (batch, index, token) =>
        {
            var response = await client.BulkAsync(descriptor =>
                    descriptor.IndexMany(batch, (operationDescriptor, o) => operationDescriptor.Index(this.IndexName)), token);
            if (response.IsValidResponse)
            {
                Console.WriteLine(
                        $"{DateTime.Now.ToString("T", CultureInfo.CurrentCulture)}: \t Uploaded batch {index} of {batch.Count} records");
            }
            else if (!response.IsValidResponse)
            {
                Console.WriteLine("Error uploading records");

                foreach (var item in response.ItemsWithErrors)
                {
                    Console.WriteLine(item.Error);
                }
            }
        }, cancellationToken);
    }


    [Option(Description = "Index name", ShortName = "i", LongName = "index")]
    public string? IndexName { get; set; } = null;

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
            var split = ApiKey.Split(':');
            return new Base64ApiKey(split[0], split[1]);
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


    [Option(Description = "CSV Delimiter", ShortName = "d", LongName = "delimiter")]
    public string Delimiter { get; set; } = ",";

    [Option(Description = "Api key", ShortName = "k", LongName = "key")]
    public string? ApiKey { get; set; }

    [Option(Description = "Cloud id", ShortName = "c", LongName = "cloud")]
    public string? CloudId { get; set; }
}

public enum PropertyNameStrategy
{
    Default,
    Lower,
    Upper,
    CamelCase,
}