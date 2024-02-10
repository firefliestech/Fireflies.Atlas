using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Fireflies.Atlas.Annotations;
using Fireflies.Atlas.Core;

namespace Fireflies.Atlas.Sources.SqlServer;

public class LambdaToSqlTranslator<T>(SqlDescriptor sqlDescriptor, Expression? expression, Expression? filter) : ExpressionVisitor, IDisposable {
    private readonly StringBuilder _sqlAccumulator = new();
    private static readonly MethodInfo? StringContainsMethodInfo = typeof(string).GetMethod(nameof(string.Contains), new[] { typeof(string) });
    private static readonly MethodInfo? StringContainsWithStringComparisonMethodInfo = typeof(string).GetMethod(nameof(string.Contains), new[] { typeof(string), typeof(StringComparison) });
    private static readonly MethodInfo? StringStartsWithMethodInfo = typeof(string).GetMethod(nameof(string.StartsWith), new[] { typeof(string) });
    private static readonly MethodInfo? StringStartsWithWithStringComparisonMethodInfo = typeof(string).GetMethod(nameof(string.StartsWith), new[] { typeof(string), typeof(StringComparison) });
    private static readonly MethodInfo? StringEndWithMethodInfo = typeof(string).GetMethod(nameof(string.EndsWith), new[] { typeof(string) });
    private static readonly MethodInfo? StringEndsWithWithStringComparisonMethodInfo = typeof(string).GetMethod(nameof(string.StartsWith), new[] { typeof(string), typeof(StringComparison) });

    private static readonly MethodInfo? StringEqualsMethodInfo = typeof(string).GetMethod(nameof(string.Equals), new[] { typeof(string) });
    private static readonly MethodInfo? StringEqualsWithStringComparisonMethodInfo = typeof(string).GetMethod(nameof(string.Equals), new[] { typeof(string), typeof(StringComparison) });

    public string Translate( ) {
        AddSelect();
        AddColumns();
        AddFrom(sqlDescriptor);

        if(expression != null) {
            AddWhere(expression);
            if(filter != null) {
                _sqlAccumulator.Append(" AND ");
                Visit(filter);
            }
        } else if(filter != null) {
            AddWhere(filter);
        }

        return _sqlAccumulator.ToString();
    }

    private void AddSelect() {
        _sqlAccumulator.Append("SELECT");
    }

    private void AddColumns() {
        var columns = typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public).Select(x => new { Property = x, Attribute = x.GetCustomAttribute<AtlasFieldAttribute>(true) })
            .Where(x => x.Attribute != null)
            .Select(x => !string.IsNullOrWhiteSpace(x.Attribute!.Name) ? $"[{x.Attribute.Name}] AS [{x.Property.Name}]" : $"[{x.Property.Name}]");

        _sqlAccumulator.Append($" {string.Join(", ", columns)}");
    }

    private void AddFrom(SqlDescriptor sqlDescriptor) {
        _sqlAccumulator.Append($" FROM {sqlDescriptor.AsSql}");
    }

    private void AddWhere(Expression expression) {
        _sqlAccumulator.Append(" WHERE ");
        Visit(expression);
    }

    protected override Expression VisitUnary(UnaryExpression u) {
        switch(u.NodeType) {
            case ExpressionType.Not:
                _sqlAccumulator.Append(" NOT ");
                Visit(u.Operand);
                break;
            case ExpressionType.Convert:
                Visit(u.Operand);
                break;
            default:
                throw new NotSupportedException($"The unary operator '{u.NodeType}' is not supported");
        }

        return u;
    }

    protected override Expression VisitBinary(BinaryExpression b) {
        _sqlAccumulator.Append("(");
        Visit(b.Left);

        switch(b.NodeType) {
            case ExpressionType.And:
                _sqlAccumulator.Append(" AND ");
                break;
            case ExpressionType.AndAlso:
                _sqlAccumulator.Append(" AND ");
                break;
            case ExpressionType.Or:
                _sqlAccumulator.Append(" OR ");
                break;
            case ExpressionType.OrElse:
                _sqlAccumulator.Append(" OR ");
                break;
            case ExpressionType.Equal:
                _sqlAccumulator.Append(IsNullConstant(b.Right) ? " IS " : " = ");
                break;
            case ExpressionType.NotEqual:
                _sqlAccumulator.Append(IsNullConstant(b.Right) ? " IS NOT " : " <> ");
                break;
            case ExpressionType.LessThan:
                _sqlAccumulator.Append(" < ");
                break;
            case ExpressionType.LessThanOrEqual:
                _sqlAccumulator.Append(" <= ");
                break;
            case ExpressionType.GreaterThan:
                _sqlAccumulator.Append(" > ");
                break;
            case ExpressionType.GreaterThanOrEqual:
                _sqlAccumulator.Append(" >= ");
                break;
            default:
                throw new NotSupportedException($"The binary operator '{b.NodeType}' is not supported");
        }

        Visit(b.Right);
        _sqlAccumulator.Append(")");

        return b;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node) {
        if(node.Object is not MemberExpression memberExpression || node.Arguments is not [ConstantExpression constantArgument, ..]) {
            throw new NotSupportedException($"The method call '{node.Method}' is not supported");
        }

        if(node.Method == StringContainsMethodInfo || node.Method == StringContainsWithStringComparisonMethodInfo) {
            Visit(memberExpression);
            _sqlAccumulator.Append($" LIKE '%{constantArgument.Value}%'");
            return node;
        }

        if(node.Method == StringStartsWithMethodInfo || node.Method == StringStartsWithWithStringComparisonMethodInfo) {
            Visit(memberExpression);
            _sqlAccumulator.Append($" LIKE '{constantArgument.Value}%'");
            return node;
        }

        if(node.Method == StringEndWithMethodInfo || node.Method == StringEndsWithWithStringComparisonMethodInfo) {
            Visit(memberExpression);
            _sqlAccumulator.Append($" LIKE '%{constantArgument.Value}'");
            return node;
        }

        if(node.Method == StringEqualsMethodInfo || node.Method == StringEqualsWithStringComparisonMethodInfo) {
            Visit(memberExpression);
            _sqlAccumulator.Append($" = '{constantArgument.Value}'");
            return node;
        }

        var methodValue = ExpressionHelper.GetValue(node);
        AddConstantValue(methodValue);

        return node;
    }

    protected override Expression VisitConstant(ConstantExpression c) {
        AddConstantValue(c.Value);
        return c;
    }

    private void AddConstantValue(object? value) {
        var q = value as IQueryable;

        if(value == null) {
            _sqlAccumulator.Append("NULL");
            return;
        }

        switch(q) {
            case null:
                var nullableType = value.GetType();
                var type = Nullable.GetUnderlyingType(nullableType) ?? nullableType;
                switch(Type.GetTypeCode(type)) {
                    case TypeCode.Boolean:
                        _sqlAccumulator.Append((bool)value ? 1 : 0);
                        break;

                    case TypeCode.SByte:
                    case TypeCode.Byte:
                    case TypeCode.Int16:
                    case TypeCode.UInt16:
                    case TypeCode.Int32:
                    case TypeCode.UInt32:
                    case TypeCode.Int64:
                    case TypeCode.UInt64:
                    case TypeCode.Single:
                    case TypeCode.Double:
                    case TypeCode.Decimal:
                        _sqlAccumulator.Append(value);
                        break;

                    case TypeCode.Object: {
                        if(type == typeof(DateTimeOffset)) {
                            _sqlAccumulator.Append("CONVERT(DateTimeOffset, '");
                            _sqlAccumulator.Append(value);
                            _sqlAccumulator.Append("')");
                        } else {
                            AppendStringValue(value);
                        }

                        break;
                    }

                    default:
                        AppendStringValue(value);
                        break;
                }

                break;
        }
    }

    protected override Expression VisitMember(MemberExpression m) {
        if(m.Expression is not { NodeType: ExpressionType.Parameter })
            throw new NotSupportedException($"The member '{m.Member.Name}' is not supported");

        var attribute = m.Member.GetCustomAttributes(typeof(AtlasFieldAttribute), true).Cast<AtlasFieldAttribute>().FirstOrDefault();
        if(attribute != null && !string.IsNullOrWhiteSpace(attribute.Name)) {
            _sqlAccumulator.Append(attribute.Name);
        } else {
            _sqlAccumulator.Append(m.Member.Name);
        }

        return m;
    }

    protected bool IsNullConstant(Expression exp) {
        return exp.NodeType == ExpressionType.Constant && ((ConstantExpression)exp).Value == null;
    }

    private void AppendStringValue(object c) {
        _sqlAccumulator.Append("'");
        _sqlAccumulator.Append(c);
        _sqlAccumulator.Append("'");
    }

    public void Dispose() {
        _sqlAccumulator.Clear();
    }
}