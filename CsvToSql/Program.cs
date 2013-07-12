using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using LumenWorks.Framework.IO.Csv;

namespace CsvToSql
{
    class Program
    {
        static void Main(string[] args)
        {
            var builder = new SqlConnectionStringBuilder
                {
                    DataSource = @"10.6.33.35\sqlexpress",
                    IntegratedSecurity = false,
                    UserID = "sa",
                    Password = "Password1",
                    InitialCatalog = "Catalog"
                };

            var stopWatch = new Stopwatch();

            var rowsStopWatch = new Stopwatch();

            stopWatch.Start();

            using (var textReader = File.OpenText(@"C:\Users\rgunawan\Downloads\dataJul-12-2013.csv"))
            using (var reader = new CsvReader(textReader, true))
            using (var conn = new SqlConnection(builder.ToString()))
            using (var sql = new SqlBulkCopy(conn))
            {
                conn.Open();

                sql.BatchSize = 10000;
                
                sql.NotifyAfter = 10000;
                sql.SqlRowsCopied +=
                    (sender, eventArgs) =>
                    {
                        Console.WriteLine("Copied: {0} rows in {1} seconds.", eventArgs.RowsCopied, rowsStopWatch.Elapsed.TotalSeconds);
                        rowsStopWatch.Restart();
                      
                    };

                sql.DestinationTableName = "TestTable";
                rowsStopWatch.Start();

                sql.WriteToServer(reader);
                
                sql.Close();
                conn.Close();
            }

            stopWatch.Stop();

            Console.WriteLine("Elapsed seconds: " + stopWatch.Elapsed.TotalSeconds);
            Console.ReadKey();
        }
    }
}
