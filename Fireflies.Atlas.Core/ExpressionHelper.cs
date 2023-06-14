using System.Linq.Expressions;

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
        if (memberExpression == null)
            return null;

        var otherExpression = left is MemberExpression ? right : left;
        if (otherExpression is ConstantExpression constantExpression)
            return (memberExpression, constantExpression);

        if (otherExpression is UnaryExpression { NodeType: ExpressionType.Convert, Operand: ConstantExpression constantConvertedExpression })
            return (memberExpression, constantConvertedExpression);

        return null;
    }
}