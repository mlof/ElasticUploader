namespace ElasticUploader.Abstractions;

public interface IAsyncEnumerableReader : IDisposable
{
    IAsyncEnumerable<dynamic> GetRecords(CancellationToken cancellationToken = default);
}