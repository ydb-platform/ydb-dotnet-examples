using System;
using System.Threading.Tasks;
using CommandLine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ydb.Sdk.Examples
{
    class CmdOptions
    {
        [Option('e', "endpoint", Required = true, HelpText = "Database endpoint")]
        public string Endpoint { get; set; } = "";

        [Option('d', "database", Required = true, HelpText = "Database name")]
        public string Database { get; set; } = "";

        [Option('p', "path", HelpText = "Base path for tables")]
        public string Path { get; set; } = "ydb-dotnet-basic";
    }

    static class Program
    {
        private static ServiceProvider GetServiceProvider()
        {
            return new ServiceCollection()
               .AddLogging(configure => configure.AddConsole().SetMinimumLevel(LogLevel.Information))
               .BuildServiceProvider();
        }

        static async Task Run(CmdOptions cmdOptions)
        {
            using var serviceProvider = GetServiceProvider();
            var loggerFactory = serviceProvider.GetService<ILoggerFactory>();

            loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;

            await BasicExample.Run(
                endpoint: cmdOptions.Endpoint,
                database: cmdOptions.Database,
                credentialsProvider: await AuthUtils.MakeCredentialsFromEnv(loggerFactory: loggerFactory),
                path: cmdOptions.Path,
                loggerFactory: loggerFactory
            );
        }

        static async Task Main(string[] args)
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            await Parser.Default.ParseArguments<CmdOptions>(args)
                .WithParsedAsync(options => Run(options));
        }
    }
}
