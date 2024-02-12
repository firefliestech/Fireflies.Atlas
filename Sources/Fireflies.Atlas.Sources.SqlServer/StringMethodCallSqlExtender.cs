using System.Linq.Expressions;
using System.Reflection;

namespace Fireflies.Atlas.Sources.SqlServer;

internal class StringMethodCallSqlExtender : IMethodCallSqlExtender {
    private static readonly MethodInfo? StringContainsMethodInfo = typeof(string).GetMethod(nameof(string.Contains), new[] { typeof(string) });
    private static readonly MethodInfo? StringContainsWithStringComparisonMethodInfo = typeof(string).GetMethod(nameof(string.Contains), new[] { typeof(string), typeof(StringComparison) });
    private static readonly MethodInfo? StringStartsWithMethodInfo = typeof(string).GetMethod(nameof(string.StartsWith), new[] { typeof(string) });
    private static readonly MethodInfo? StringStartsWithWithStringComparisonMethodInfo = typeof(string).GetMethod(nameof(string.StartsWith), new[] { typeof(string), typeof(StringComparison) });
    private static readonly MethodInfo? StringEndWithMethodInfo = typeof(string).GetMethod(nameof(string.EndsWith), new[] { typeof(string) });
    private static readonly MethodInfo? StringEndsWithWithStringComparisonMethodInfo = typeof(string).GetMethod(nameof(string.StartsWith), new[] { typeof(string), typeof(StringComparison) });

    private static readonly MethodInfo? StringEqualsMethodInfo = typeof(string).GetMethod(nameof(string.Equals), new[] { typeof(string) });
    private static readonly MethodInfo? StringEqualsWithStringComparisonMethodInfo = typeof(string).GetMethod(nameof(string.Equals), new[] { typeof(string), typeof(StringComparison) });

    public (bool, string?) Handle(MethodCallExpression node) {
        if(node.Arguments is not [ConstantExpression constantArgument, ..]) {
            return (false, null);
        }

        if(node.Method == StringContainsMethodInfo || node.Method == StringContainsWithStringComparisonMethodInfo) {
            return (true, $" LIKE '%{constantArgument.Value}%'");
        }

        if(node.Method == StringStartsWithMethodInfo || node.Method == StringStartsWithWithStringComparisonMethodInfo) {
            return (true, $" LIKE '{constantArgument.Value}%'");
        }

        if(node.Method == StringEndWithMethodInfo || node.Method == StringEndsWithWithStringComparisonMethodInfo) {
            return (true, $" LIKE '%{constantArgument.Value}'");
        }

        if(node.Method == StringEqualsMethodInfo || node.Method == StringEqualsWithStringComparisonMethodInfo) {
            return (true, $" = '{constantArgument.Value}'");
        }

        return (false, null);
    }
}