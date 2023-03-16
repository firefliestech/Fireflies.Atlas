using System.Linq.Expressions;
using System.Reflection;

namespace Fireflies.Atlas.Core;

public class PredicateToDocument {
    public static TDocument CreateDocument<TDocument>(Expression<Func<TDocument, bool>> exp) where TDocument : new() {
        var document = new TDocument();

        var documentVisitor = new DocumentVisitor<TDocument>(exp, document);
        documentVisitor.Visit(exp);

        return document;
    }

    private class DocumentVisitor<TDocument> : ExpressionVisitor {
        private readonly TDocument _document;

        public DocumentVisitor(Expression<Func<TDocument, bool>> expression, TDocument document) {
            _document = document;
        }

        protected override Expression VisitBinary(BinaryExpression node) {
            if(node.NodeType != ExpressionType.Equal)
                return base.VisitBinary(node);

            ConstantExpression? constantExpression = null;
            MemberExpression? memberExpression = null;

            switch(node.Left) {
                case ConstantExpression leftConstant when node.Right is MemberExpression rightMember:
                    constantExpression = leftConstant;
                    memberExpression = rightMember;
                    break;
                case MemberExpression leftMember when node.Right is ConstantExpression rightConstant:
                    constantExpression = rightConstant;
                    memberExpression = leftMember;
                    break;
            }

            if(constantExpression == null || memberExpression == null)
                return node;

            var propertyInfo = (PropertyInfo)memberExpression.Member;
            propertyInfo.SetValue(_document, constantExpression.Value);

            return node;

        }
    }
}