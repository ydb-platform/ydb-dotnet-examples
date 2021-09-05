using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Table;

namespace Ydb.Sdk.Examples
{
    class BasicExample : TableExampleBase {
        private BasicExample(TableClient client, string database, string path)
            : base(client, database, path)
        {
        }

        private async Task CreateTables() {
            var response = await Client.SessionExec(async session =>
            {
                return await session.ExecuteSchemeQuery(@$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    create table series (
                        series_id uint64,
                        title utf8,
                        series_info utf8,
                        release_date date,
                        primary key (series_id)
                    );

                    create table seasons (
                        series_id uint64,
                        season_id uint64,
                        title utf8,
                        first_aired date,
                        last_aired date,
                        primary key (series_id, season_id)
                    );

                    create table episodes (
                        series_id uint64,
                        season_id uint64,
                        episode_id uint64,
                        title utf8,
                        air_date date,
                        primary key (series_id, season_id, episode_id)
                    );
                ");
            });

            response.Status.EnsureSuccess();
        }

        public static async Task Run(
            string endpoint,
            string database,
            string path,
            ILoggerFactory loggerFactory)
        {
            var config = new DriverConfig(
                endpoint: endpoint,
                database: database,
                credentials: null
            );

            using var driver = new Driver(
                config: config,
                loggerFactory: loggerFactory
            );

            await driver.Initialize();

            using var tableClient = new TableClient(driver, new TableClientConfig());

            var example = new BasicExample(tableClient, database, path);
            await example.CreateTables();
        }
    }
}
