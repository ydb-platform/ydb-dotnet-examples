using Ydb.Sdk.Table;

public class TableExampleBase {
    protected TableClient Client { get; }
    protected string BasePath { get; }

    protected TableExampleBase(TableClient client, string database, string path) {
        Client = client;
        BasePath = string.Join('/', new [] {database, path});
    }
}
