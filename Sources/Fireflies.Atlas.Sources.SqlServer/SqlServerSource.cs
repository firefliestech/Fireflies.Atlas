using System.Linq.Expressions;
using Dapper;
using Fireflies.Atlas.Core;
using Fireflies.Atlas.Sources.SqlServer.Monitor;
using Fireflies.Logging.Abstractions;
using Microsoft.Data.SqlClient;

namespace Fireflies.Atlas.Sources.SqlServer;

public class SqlServerSource : IDisposable {
    private readonly string _connectionString;
    private readonly IReadOnlyCollection<IMethodCallSqlExtender> _methodCallExtenders;
    private readonly IFirefliesLogger _logger;

    public SqlMonitor UpdateFetcher { get; }
    public Core.Atlas Atlas { get; }

    public SqlServerSource(Core.Atlas atlas, string connectionString, IReadOnlyCollection<IMethodCallSqlExtender>? methodExtenders = null) {
        _connectionString = connectionString;
        _methodCallExtenders = new List<IMethodCallSqlExtender>(methodExtenders ?? Array.Empty<IMethodCallSqlExtender>()) { new StringMethodCallSqlExtender() };
        _logger = atlas.LoggerFactory.GetLogger<SqlServerSource>();

        Atlas = atlas;
        UpdateFetcher = new SqlMonitor(_connectionString, atlas);
    }

    public async Task<IEnumerable<TDocument>> GetDocuments<TDocument>(Expression<Func<TDocument, bool>>? predicate, SqlDescriptor sqlDescriptor, Expression<Func<TDocument, bool>>? filter, ExecutionFlags flags) where TDocument : new() {
        using var lambdaToSqlTranslator = new LambdaToSqlTranslator<TDocument>(_methodCallExtenders, sqlDescriptor, predicate, flags.HasFlag(ExecutionFlags.BypassFilter) ? null : filter);
        var query = lambdaToSqlTranslator.Translate();
        await using var connection = new SqlConnection(_connectionString);
        connection.Open();
        return (await connection.QueryAsync<TDocument>(query).ConfigureAwait(false)).ToArray();
    }

    public void Dispose() {
        UpdateFetcher.Dispose();
    }
}