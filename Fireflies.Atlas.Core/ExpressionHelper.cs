using System.Linq.Expressions;
using System.Reflection;

namespace Fireflies.Atlas.Core;

public static class ExpressionHelper {
    public static (MemberExpression MemberExpression, ConstantExpression ConstantExpression)? GetMemberAndConstant(BinaryExpression binaryExpression) {
        var left = binaryExpression.Left;
        if(left is UnaryExpression { NodeType: ExpressionType.Convert } leftUnary) {
            left = leftUnary.Operand;
        }

        var right = binaryExpression.Right;
        if(right is UnaryExpression { NodeType: ExpressionType.Convert } rightUnary)
            right = rightUnary.Operand;

        var memberExpression = left as MemberExpression ?? right as MemberExpression;
        if(memberExpression == null)
            return null;

        var otherExpression = left is MemberExpression ? right : left;
        if(otherExpression is ConstantExpression constantExpression)
            return (memberExpression, constantExpression);

        if(otherExpression is UnaryExpression { NodeType: ExpressionType.Convert, Operand: ConstantExpression constantConvertedExpression })
            return (memberExpression, constantConvertedExpression);

        return null;
    }

    public static object? GetValue(Expression expression) {
        return GetValue(expression, true);
    }

    public static object? GetValueWithoutCompiling(Expression expression) {
        return GetValue(expression, false);
    }

    private static object? GetValue(Expression? expression, bool allowCompile) {
        switch(expression) {
            case null:
                return null;
            case ConstantExpression constantExpression:
                return GetValue(constantExpression);
            case MemberExpression memberExpression:
                return GetValue(memberExpression, allowCompile);
            case MethodCallExpression methodCallExpression:
                return GetValue(methodCallExpression, allowCompile);
        }

        if(allowCompile) {
            return GetValueUsingCompile(expression);
        }

        throw new Exception("Couldn't evaluate Expression without compiling: " + expression);
    }

    private static object? GetValue(ConstantExpression constantExpression) {
        return constantExpression.Value;
    }

    private static object? GetValue(MemberExpression memberExpression, bool allowCompile) {
        var value = GetValue(memberExpression.Expression, allowCompile);

        var member = memberExpression.Member;
        if(member is FieldInfo fieldInfo) {
            return fieldInfo.GetValue(value);
        }

        if(member is PropertyInfo propertyInfo) {
            try {
                return propertyInfo.GetValue(value);
            } catch(TargetInvocationException e) {
                throw e.InnerException;
            }
        }

        throw new Exception("Unknown member type: " + member.GetType());
    }

    private static object? GetValue(MethodCallExpression methodCallExpression, bool allowCompile) {
        var paras = GetArray(methodCallExpression.Arguments, true);
        var obj = GetValue(methodCallExpression.Object, allowCompile);

        try {
            return methodCallExpression.Method.Invoke(obj, paras);
        } catch(TargetInvocationException e) {
            throw e.InnerException;
        }
    }

    private static object?[] GetArray(IEnumerable<Expression> expressions, bool allowCompile) {
        return expressions.Select(expression => GetValue(expression, allowCompile)).ToArray();
    }

    public static object GetValueUsingCompile(Expression expression) {
        var lambdaExpression = Expression.Lambda(expression);
        var dele = lambdaExpression.Compile();
        return dele.DynamicInvoke();
    }
}