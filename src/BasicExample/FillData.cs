using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Ydb.Sdk.Table;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Examples
{
    partial class BasicExample
    {
        // Fill sample tables with initial data.
        private async Task FillData() {
            var response = await Client.SessionExec(async session =>
            {
                var query = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    DECLARE $seriesData AS List<Struct<
                        series_id: Uint64,
                        title: Utf8,
                        series_info: Utf8,
                        release_date: Date>>;

                    DECLARE $seasonsData AS List<Struct<
                        series_id: Uint64,
                        season_id: Uint64,
                        title: Utf8,
                        first_aired: Date,
                        last_aired: Date>>;

                    DECLARE $episodesData AS List<Struct<
                        series_id: Uint64,
                        season_id: Uint64,
                        episode_id: Uint64,
                        title: Utf8,
                        air_date: Date>>;

                    REPLACE INTO series
                    SELECT * FROM AS_TABLE($seriesData);

                    REPLACE INTO seasons
                    SELECT * FROM AS_TABLE($seasonsData);

                    REPLACE INTO episodes
                    SELECT * FROM AS_TABLE($episodesData);
                ";

                return await session.ExecuteDataQuery(
                    query: query,
                    txControl: TxControl.BeginSerializableRW().Commit(),
                    parameters: GetDataParams(),
                    settings: DefaultDataQuerySettings
                );
            });

            response.Status.EnsureSuccess();
        }

        private static Dictionary<string, YdbValue> GetDataParams() {
            var series = new [] {
                new { SeriesId = 1, Title = "IT Crowd", ReleaseDate = DateTime.Parse("2006-02-03"),
                    Info = "The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, " +
                           "produced by Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, " +
                           "and Matt Berry."},
                new { SeriesId = 2, Title = "Silicon Valley", ReleaseDate = DateTime.Parse("2014-04-06"),
                    Info = "Silicon Valley is an American comedy television series created by Mike Judge, " +
                           "John Altschuler and Dave Krinsky. The series focuses on five young men who founded " +
                           "a startup company in Silicon Valley." },
            };

            var seasons = new [] {
                new { SeriesId = 1, SeasonId = 1, Title = "Season 1", FirstAired = DateTime.Parse("2006-02-03"), LastAired = DateTime.Parse("2006-03-03") },
                new { SeriesId = 1, SeasonId = 2, Title = "Season 2", FirstAired = DateTime.Parse("2007-08-24"), LastAired = DateTime.Parse("2007-09-28") },
                new { SeriesId = 1, SeasonId = 3, Title = "Season 3", FirstAired = DateTime.Parse("2008-11-21"), LastAired = DateTime.Parse("2008-12-26") },
                new { SeriesId = 1, SeasonId = 4, Title = "Season 4", FirstAired = DateTime.Parse("2010-06-25"), LastAired = DateTime.Parse("2010-07-30") },
                new { SeriesId = 2, SeasonId = 1, Title = "Season 1", FirstAired = DateTime.Parse("2014-04-06"), LastAired = DateTime.Parse("2014-06-01") },
                new { SeriesId = 2, SeasonId = 2, Title = "Season 2", FirstAired = DateTime.Parse("2015-04-12"), LastAired = DateTime.Parse("2015-06-14") },
                new { SeriesId = 2, SeasonId = 3, Title = "Season 3", FirstAired = DateTime.Parse("2016-04-24"), LastAired = DateTime.Parse("2016-06-26") },
                new { SeriesId = 2, SeasonId = 4, Title = "Season 4", FirstAired = DateTime.Parse("2017-04-23"), LastAired = DateTime.Parse("2017-06-25") },
                new { SeriesId = 2, SeasonId = 5, Title = "Season 5", FirstAired = DateTime.Parse("2018-03-25"), LastAired = DateTime.Parse("2018-05-13") },
            };

            var episodes = new [] {
                new { SeriesId = 1, SeasonId = 1, EpisodeId = 1, Title = "Yesterday's Jam", AirDate = DateTime.Parse("2006-02-03") },
                new { SeriesId = 1, SeasonId = 1, EpisodeId = 2, Title = "Calamity Jen", AirDate = DateTime.Parse("2006-02-03") },
                new { SeriesId = 1, SeasonId = 1, EpisodeId = 3, Title = "Fifty-Fifty", AirDate = DateTime.Parse("2006-02-10") },
                new { SeriesId = 1, SeasonId = 1, EpisodeId = 4, Title = "The Red Door", AirDate = DateTime.Parse("2006-02-17") },
                new { SeriesId = 1, SeasonId = 1, EpisodeId = 5, Title = "The Haunting of Bill Crouse", AirDate = DateTime.Parse("2006-02-24") },
                new { SeriesId = 1, SeasonId = 1, EpisodeId = 6, Title = "Aunt Irma Visits", AirDate = DateTime.Parse("2006-03-03") },
                new { SeriesId = 1, SeasonId = 2, EpisodeId = 1, Title = "The Work Outing", AirDate = DateTime.Parse("2006-08-24") },
                new { SeriesId = 1, SeasonId = 2, EpisodeId = 2, Title = "Return of the Golden Child", AirDate = DateTime.Parse("2007-08-31") },
                new { SeriesId = 1, SeasonId = 2, EpisodeId = 3, Title = "Moss and the German", AirDate = DateTime.Parse("2007-09-07") },
                new { SeriesId = 1, SeasonId = 2, EpisodeId = 4, Title = "The Dinner Party", AirDate = DateTime.Parse("2007-09-14") },
                new { SeriesId = 1, SeasonId = 2, EpisodeId = 5, Title = "Smoke and Mirrors", AirDate = DateTime.Parse("2007-09-21") },
                new { SeriesId = 1, SeasonId = 2, EpisodeId = 6, Title = "Men Without Women", AirDate = DateTime.Parse("2007-09-28") },
                new { SeriesId = 1, SeasonId = 3, EpisodeId = 1, Title = "From Hell", AirDate = DateTime.Parse("2008-11-21") },
                new { SeriesId = 1, SeasonId = 3, EpisodeId = 2, Title = "Are We Not Men?", AirDate = DateTime.Parse("2008-11-28") },
                new { SeriesId = 1, SeasonId = 3, EpisodeId = 3, Title = "Tramps Like Us", AirDate = DateTime.Parse("2008-12-05") },
                new { SeriesId = 1, SeasonId = 3, EpisodeId = 4, Title = "The Speech", AirDate = DateTime.Parse("2008-12-12") },
                new { SeriesId = 1, SeasonId = 3, EpisodeId = 5, Title = "Friendface", AirDate = DateTime.Parse("2008-12-19") },
                new { SeriesId = 1, SeasonId = 3, EpisodeId = 6, Title = "Calendar Geeks", AirDate = DateTime.Parse("2008-12-26") },
                new { SeriesId = 1, SeasonId = 4, EpisodeId = 1, Title = "Jen The Fredo", AirDate = DateTime.Parse("2010-06-25") },
                new { SeriesId = 1, SeasonId = 4, EpisodeId = 2, Title = "The Final Countdown", AirDate = DateTime.Parse("2010-07-02") },
                new { SeriesId = 1, SeasonId = 4, EpisodeId = 3, Title = "Something Happened", AirDate = DateTime.Parse("2010-07-09") },
                new { SeriesId = 1, SeasonId = 4, EpisodeId = 4, Title = "Italian For Beginners", AirDate = DateTime.Parse("2010-07-16") },
                new { SeriesId = 1, SeasonId = 4, EpisodeId = 5, Title = "Bad Boys", AirDate = DateTime.Parse("2010-07-23") },
                new { SeriesId = 1, SeasonId = 4, EpisodeId = 6, Title = "Reynholm vs Reynholm", AirDate = DateTime.Parse("2010-07-30") },
                new { SeriesId = 2, SeasonId = 1, EpisodeId = 1, Title = "Minimum Viable Product", AirDate = DateTime.Parse("2014-04-06") },
                new { SeriesId = 2, SeasonId = 1, EpisodeId = 2, Title = "The Cap Table", AirDate = DateTime.Parse("2014-04-13") },
                new { SeriesId = 2, SeasonId = 1, EpisodeId = 3, Title = "Articles of Incorporation", AirDate = DateTime.Parse("2014-04-20") },
                new { SeriesId = 2, SeasonId = 1, EpisodeId = 4, Title = "Fiduciary Duties", AirDate = DateTime.Parse("2014-04-27") },
                new { SeriesId = 2, SeasonId = 1, EpisodeId = 5, Title = "Signaling Risk", AirDate = DateTime.Parse("2014-05-04") },
                new { SeriesId = 2, SeasonId = 1, EpisodeId = 6, Title = "Third Party Insourcing", AirDate = DateTime.Parse("2014-05-11") },
                new { SeriesId = 2, SeasonId = 1, EpisodeId = 7, Title = "Proof of Concept", AirDate = DateTime.Parse("2014-05-18") },
                new { SeriesId = 2, SeasonId = 1, EpisodeId = 8, Title = "Optimal Tip-to-Tip Efficiency", AirDate = DateTime.Parse("2014-06-01") },
                new { SeriesId = 2, SeasonId = 2, EpisodeId = 1, Title = "Sand Hill Shuffle", AirDate = DateTime.Parse("2015-04-12") },
                new { SeriesId = 2, SeasonId = 2, EpisodeId = 2, Title = "Runaway Devaluation", AirDate = DateTime.Parse("2015-04-19") },
                new { SeriesId = 2, SeasonId = 2, EpisodeId = 3, Title = "Bad Money", AirDate = DateTime.Parse("2015-04-26") },
                new { SeriesId = 2, SeasonId = 2, EpisodeId = 4, Title = "The Lady", AirDate = DateTime.Parse("2015-05-03") },
                new { SeriesId = 2, SeasonId = 2, EpisodeId = 5, Title = "Server Space", AirDate = DateTime.Parse("2015-05-10") },
                new { SeriesId = 2, SeasonId = 2, EpisodeId = 6, Title = "Homicide", AirDate = DateTime.Parse("2015-05-17") },
                new { SeriesId = 2, SeasonId = 2, EpisodeId = 7, Title = "Adult Content", AirDate = DateTime.Parse("2015-05-24") },
                new { SeriesId = 2, SeasonId = 2, EpisodeId = 8, Title = "White Hat/Black Hat", AirDate = DateTime.Parse("2015-05-31") },
                new { SeriesId = 2, SeasonId = 2, EpisodeId = 9, Title = "Binding Arbitration", AirDate = DateTime.Parse("2015-06-07") },
                new { SeriesId = 2, SeasonId = 2, EpisodeId = 10,Title =  "Two Days of the Condor", AirDate = DateTime.Parse("2015-06-14") },
                new { SeriesId = 2, SeasonId = 3, EpisodeId = 1, Title = "Founder Friendly", AirDate = DateTime.Parse("2016-04-24") },
                new { SeriesId = 2, SeasonId = 3, EpisodeId = 2, Title = "Two in the Box", AirDate = DateTime.Parse("2016-05-01") },
                new { SeriesId = 2, SeasonId = 3, EpisodeId = 3, Title = "Meinertzhagen's Haversack", AirDate = DateTime.Parse("2016-05-08") },
                new { SeriesId = 2, SeasonId = 3, EpisodeId = 4, Title = "Maleant Data Systems Solutions", AirDate = DateTime.Parse("2016-05-15") },
                new { SeriesId = 2, SeasonId = 3, EpisodeId = 5, Title = "The Empty Chair", AirDate = DateTime.Parse("2016-05-22") },
                new { SeriesId = 2, SeasonId = 3, EpisodeId = 6, Title = "Bachmanity Insanity", AirDate = DateTime.Parse("2016-05-29") },
                new { SeriesId = 2, SeasonId = 3, EpisodeId = 7, Title = "To Build a Better Beta", AirDate = DateTime.Parse("2016-06-05") },
                new { SeriesId = 2, SeasonId = 3, EpisodeId = 8, Title = "Bachman's Earnings Over-Ride", AirDate = DateTime.Parse("2016-06-12") },
                new { SeriesId = 2, SeasonId = 3, EpisodeId = 9, Title = "Daily Active Users", AirDate = DateTime.Parse("2016-06-19") },
                new { SeriesId = 2, SeasonId = 3, EpisodeId = 10,Title =  "The Uptick", AirDate = DateTime.Parse("2016-06-26") },
                new { SeriesId = 2, SeasonId = 4, EpisodeId = 1, Title = "Success Failure", AirDate = DateTime.Parse("2017-04-23") },
                new { SeriesId = 2, SeasonId = 4, EpisodeId = 2, Title = "Terms of Service", AirDate = DateTime.Parse("2017-04-30") },
                new { SeriesId = 2, SeasonId = 4, EpisodeId = 3, Title = "Intellectual Property", AirDate = DateTime.Parse("2017-05-07") },
                new { SeriesId = 2, SeasonId = 4, EpisodeId = 4, Title = "Teambuilding Exercise", AirDate = DateTime.Parse("2017-05-14") },
                new { SeriesId = 2, SeasonId = 4, EpisodeId = 5, Title = "The Blood Boy", AirDate = DateTime.Parse("2017-05-21") },
                new { SeriesId = 2, SeasonId = 4, EpisodeId = 6, Title = "Customer Service", AirDate = DateTime.Parse("2017-05-28") },
                new { SeriesId = 2, SeasonId = 4, EpisodeId = 7, Title = "The Patent Troll", AirDate = DateTime.Parse("2017-06-04") },
                new { SeriesId = 2, SeasonId = 4, EpisodeId = 8, Title = "The Keenan Vortex", AirDate = DateTime.Parse("2017-06-11") },
                new { SeriesId = 2, SeasonId = 4, EpisodeId = 9, Title = "Hooli-Con", AirDate = DateTime.Parse("2017-06-18") },
                new { SeriesId = 2, SeasonId = 4, EpisodeId = 10,Title =  "Server Error", AirDate = DateTime.Parse("2017-06-25") },
                new { SeriesId = 2, SeasonId = 5, EpisodeId = 1, Title = "Grow Fast or Die Slow", AirDate = DateTime.Parse("2018-03-25") },
                new { SeriesId = 2, SeasonId = 5, EpisodeId = 2, Title = "Reorientation", AirDate = DateTime.Parse("2018-04-01") },
                new { SeriesId = 2, SeasonId = 5, EpisodeId = 3, Title = "Chief Operating Officer", AirDate = DateTime.Parse("2018-04-08") },
                new { SeriesId = 2, SeasonId = 5, EpisodeId = 4, Title = "Tech Evangelist", AirDate = DateTime.Parse("2018-04-15") },
                new { SeriesId = 2, SeasonId = 5, EpisodeId = 5, Title = "Facial Recognition", AirDate = DateTime.Parse("2018-04-22") },
                new { SeriesId = 2, SeasonId = 5, EpisodeId = 6, Title = "Artificial Emotional Intelligence", AirDate = DateTime.Parse("2018-04-29") },
                new { SeriesId = 2, SeasonId = 5, EpisodeId = 7, Title = "Initial Coin Offering", AirDate = DateTime.Parse("2018-05-06") },
                new { SeriesId = 2, SeasonId = 5, EpisodeId = 8, Title = "Fifty-One Percent", AirDate = DateTime.Parse("2018-05-13") },
            };

            var seriesData = series.Select(s => YdbValue.MakeStruct(new Dictionary<string, YdbValue>
                {
                    { "series_id", YdbValue.MakeUint64((ulong)s.SeriesId) },
                    { "title", YdbValue.MakeUtf8(s.Title) },
                    { "series_info", YdbValue.MakeUtf8(s.Info) },
                    { "release_date", YdbValue.MakeDate(s.ReleaseDate) },
                })).ToList();

            var seasonsData = seasons.Select(s => YdbValue.MakeStruct(new Dictionary<string, YdbValue>
                {
                    { "series_id", YdbValue.MakeUint64((ulong)s.SeriesId) },
                    { "season_id", YdbValue.MakeUint64((ulong)s.SeasonId) },
                    { "title", YdbValue.MakeUtf8(s.Title) },
                    { "first_aired", YdbValue.MakeDate(s.FirstAired) },
                    { "last_aired", YdbValue.MakeDate(s.LastAired) },
                })).ToList();

            var episodesData = episodes.Select(e => YdbValue.MakeStruct(new Dictionary<string, YdbValue>
                {
                    { "series_id", YdbValue.MakeUint64((ulong)e.SeriesId) },
                    { "season_id", YdbValue.MakeUint64((ulong)e.SeasonId) },
                    { "episode_id", YdbValue.MakeUint64((ulong)e.EpisodeId) },
                    { "title", YdbValue.MakeUtf8(e.Title) },
                    { "air_date", YdbValue.MakeDate(e.AirDate) },
                })).ToList();

            return new Dictionary<string, YdbValue>()
            {
                { "$seriesData", YdbValue.MakeList(seriesData) },
                { "$seasonsData", YdbValue.MakeList(seasonsData) },
                { "$episodesData", YdbValue.MakeList(episodesData) },
            };
        }
    }
}
