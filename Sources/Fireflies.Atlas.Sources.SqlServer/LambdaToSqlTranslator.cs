using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Fireflies.Atlas.Annotations;

namespace Fireflies.Atlas.Sources.SqlServer;

public class LambdaToSqlTranslator<T> : ExpressionVisitor, IDisposable {
    private readonly StringBuilder _sqlAccumulator = new();
    private static readonly MethodInfo? StringContainsMethodInfo = typeof(string).GetMethod(nameof(string.Contains), new[] { typeof(string) });
    private static readonly MethodInfo? StringStartsWithMethodInfo = typeof(string).GetMethod(nameof(string.StartsWith), new[] { typeof(string) });
    private static readonly MethodInfo? StringEndWithMethodInfo = typeof(string).GetMethod(nameof(string.EndsWith), new[] { typeof(string) });

    public string Translate(TableDescriptor tableDescriptor, Expression? expression) {
        AddSelect();
        AddColumns();
        AddFrom(tableDescriptor);

        if (expression != null)
            AddWhere(expression);

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

    private void AddFrom(TableDescriptor tableDescriptor) {
        _sqlAccumulator.Append($" FROM {tableDescriptor.Schema}.{tableDescriptor.Table}");
    }

    private void AddWhere(Expression expression) {
        _sqlAccumulator.Append(" WHERE ");
        Visit(expression);
    }

    protected override Expression VisitUnary(UnaryExpression u) {
        switch (u.NodeType) {
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

        switch (b.NodeType) {
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
        if (node.Object is not MemberExpression memberExpression || node.Arguments.Count != 1 || node.Arguments[0] is not ConstantExpression constantArgument) {
            throw new NotSupportedException($"The method call '{node.Method}' is not supported");
        }

        if (node.Method == StringContainsMethodInfo) {
            Visit(memberExpression);
            _sqlAccumulator.Append($" LIKE '%{constantArgument.Value}%'");
            return node;
        }

        if (node.Method == StringStartsWithMethodInfo) {
            Visit(memberExpression);
            _sqlAccumulator.Append($" LIKE '{constantArgument.Value}%'");
            return node;
        }

        if (node.Method == StringEndWithMethodInfo) {
            Visit(memberExpression);
            _sqlAccumulator.Append($" LIKE '%{constantArgument.Value}'");
            return node;
        }

        throw new NotSupportedException($"The method call '{node.Method}' is not supported");
    }

    protected override Expression VisitConstant(ConstantExpression c) {
        var q = c.Value as IQueryable;

        switch (q) {
            case null when c.Value == null:
                _sqlAccumulator.Append("NULL");
                break;
            case null:
                switch (Type.GetTypeCode(c.Value.GetType())) {
                    case TypeCode.Boolean:
                        _sqlAccumulator.Append((bool)c.Value ? 1 : 0);
                        break;

                    case TypeCode.String:
                        _sqlAccumulator.Append("'");
                        _sqlAccumulator.Append(c.Value);
                        _sqlAccumulator.Append("'");
                        break;

                    case TypeCode.DateTime:
                        _sqlAccumulator.Append("'");
                        _sqlAccumulator.Append(c.Value);
                        _sqlAccumulator.Append("'");
                        break;

                    case TypeCode.Object:
                        throw new NotSupportedException($"The constant for '{c.Value}' is not supported");

                    default:
                        _sqlAccumulator.Append(c.Value);
                        break;
                }

                break;
        }

        return c;
    }

    protected override Expression VisitMember(MemberExpression m) {
        if (m.Expression is not { NodeType: ExpressionType.Parameter })
            throw new NotSupportedException($"The member '{m.Member.Name}' is not supported");

        var attribute = m.Member.GetCustomAttributes(typeof(AtlasFieldAttribute), true).Cast<AtlasFieldAttribute>().FirstOrDefault();
        if (attribute != null && !string.IsNullOrWhiteSpace(attribute.Name)) {
            _sqlAccumulator.Append(attribute.Name);
        } else {
            _sqlAccumulator.Append(m.Member.Name);
        }

        return m;
    }

    protected bool IsNullConstant(Expression exp) {
        return (exp.NodeType == ExpressionType.Constant && ((ConstantExpression)exp).Value == null);
    }

    public void Dispose() {
        _sqlAccumulator.Clear();
    }
}