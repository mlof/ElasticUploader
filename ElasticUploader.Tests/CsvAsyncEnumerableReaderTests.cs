using ElasticUploader;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.IO;
using FluentAssertions;

namespace ElasticUploader.Tests
{
    [TestClass]
    public class CsvAsyncEnumerableReaderTests
    {
        [TestMethod]
        public async Task CanGetRecords_ForCamelCase()
        {
            // Arrange
            var result = await GetRecords(",", PropertyNameStrategy.CamelCase);


            // Assert

            result.Should().HaveCount(1);

            var record = result.Single() as IDictionary<string, object>;

            record.Should().ContainKey("id");
            record.Should().ContainKey("name");

            record.Should().ContainKey("favouritePizza");

            record["id"].Should().Be("1");
            record["name"].Should().Be("John");
            record["favouritePizza"].Should().Be("Pineapple");
        }

        [TestMethod]
        public async Task CanGetRecords_ForLower()
        {
            // Arrange
            var result = await GetRecords(",", PropertyNameStrategy.Lower);

            result.Should().HaveCount(1);

            var record = result.Single() as IDictionary<string, object>;

            record.Should().ContainKey("id");

            record.Should().ContainKey("name");

            record.Should().ContainKey("favourite pizza");

            record["id"].Should().Be("1");

            record["name"].Should().Be("John");

            record["favourite pizza"].Should().Be("Pineapple");
        }

        [TestMethod]
        public async Task CanGetRecords_ForUpper()
        {
            // Arrange
            var result = await GetRecords(",", PropertyNameStrategy.Upper);

            result.Should().HaveCount(1);

            var record = result.Single() as IDictionary<string, object>;

            record.Should().ContainKey("ID");

            record.Should().ContainKey("NAME");

            record.Should().ContainKey("FAVOURITE PIZZA");

            record["ID"].Should().Be("1");

            record["NAME"].Should().Be("John");

            record["FAVOURITE PIZZA"].Should().Be("Pineapple");
        }

        private static async Task<List<dynamic>> GetRecords(string delimiter, PropertyNameStrategy strategy)
        {
            var stream = new MemoryStream();
            var writer = new StreamWriter(stream);
            await writer.WriteLineAsync("Id,Name,Favourite Pizza");
            await writer.WriteLineAsync("1,John,Pineapple");
            await writer.FlushAsync();
            stream.Position = 0;
            var csvAsyncEnumerableReader = new CsvAsyncEnumerableReader(
                stream,
                delimiter,
                strategy);


            // Act
            var records = csvAsyncEnumerableReader.GetRecords();
            var result = await records.ToListAsync();
            return result;
        }
    }
}