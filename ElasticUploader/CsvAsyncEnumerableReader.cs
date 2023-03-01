using System.Globalization;
using System.Text.Json;
using CsvHelper;
using CsvHelper.Configuration;
using ElasticUploader.Abstractions;

namespace ElasticUploader;

internal class CsvAsyncEnumerableReader : IAsyncEnumerableReader
{
    private readonly CsvReader _csvReader;
    private readonly StreamReader _reader;

    public CsvAsyncEnumerableReader(Stream stream, string delimiter, PropertyNameStrategy propertyNameFormatting)
    {
        _reader = new StreamReader(stream);
        var csvConfiguration = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            Delimiter = delimiter,
            IgnoreBlankLines = true,
            TrimOptions = TrimOptions.Trim,
            PrepareHeaderForMatch = propertyNameFormatting switch
            {
                PropertyNameStrategy.CamelCase => args =>
                    JsonNamingPolicy.CamelCase.ConvertName(args.Header).Replace(" ", ""),
                PropertyNameStrategy.Lower => args => args.Header.ToLower(CultureInfo.InvariantCulture),
                PropertyNameStrategy.Upper => args => args.Header.ToUpper(CultureInfo.InvariantCulture),
                _ => throw new ArgumentOutOfRangeException()
            }
        };

        _csvReader = new CsvReader(_reader, csvConfiguration);
    }


    public IAsyncEnumerable<dynamic> GetRecords(CancellationToken cancellationToken = default)
    {
        return _csvReader.GetRecordsAsync<dynamic>(cancellationToken);
    }

    public void Dispose()
    {
        _reader.Dispose();
        _csvReader.Dispose();
    }
}