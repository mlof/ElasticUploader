# Elastic Uploader

A simple tool to upload a csv file to an Elasticsearch index.

If index name is not specified, the file name will be used.

Authentication can be done with either a cloud id and api key or a elastic uri and user/password.

When in doubt, run with -h to get help.

## Example usage

```bat
elastic-upload -f C:\temp\test.csv -e "http://localhost:9200" -u elastic -p changeme -b 1000

elastic-upload -f C:\temp\test.csv -c cloudid:cloudid -k apikey -b 1000
```

## Performance tuning

The tool uses a bulk upload to send the data to Elasticsearch. The default batch size is 1000. This can be changed with the -b parameter. It awaits all bulk actions to be completed before sending the next batch. I don't want to set your server on fire.

Official documentation says that the Bulk API wil comfortably handle 5-15 mb per bulk. I don't know how wide your data is, so you might want to adjust the batch size accordingly.

Dynamic bulk sizing does sound interesting, so it might be added in the future.

