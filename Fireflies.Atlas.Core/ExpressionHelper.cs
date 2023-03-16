using System.Linq.Expressions;

namespace Fireflies.Atlas.Core;

public static class ExpressionHelper {
    public static (MemberExpression MemberExpression, ConstantExpression ConstantExpression)? GetMemberAndConstant(BinaryExpression binaryExpression) {
        var memberExpression = binaryExpression.Left as MemberExpression ?? binaryExpression.Right as MemberExpression;
        if (memberExpression == null)
            return null;

        var otherExpression = binaryExpression.Left is MemberExpression ? binaryExpression.Right : binaryExpression.Left;
        if (otherExpression is ConstantExpression constantExpression)
            return (memberExpression, constantExpression);

        if (otherExpression is UnaryExpression { NodeType: ExpressionType.Convert, Operand: ConstantExpression constantConvertedExpression }) {
            return (memberExpression, constantConvertedExpression);
        }

        return null;
    }
}