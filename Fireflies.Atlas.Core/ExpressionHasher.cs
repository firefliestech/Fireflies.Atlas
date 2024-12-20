using System.Linq.Expressions;
using System.Reflection;

namespace Fireflies.Atlas.Core;

internal static class ExpressionHasher {
    private const int NullHashValue = 0x61E04917;

    [ThreadStatic]
    private static HashVisitor? _visitor;

    public static int GetHashCode(Expression? e) {
        if(e == null)
            return NullHashValue;

        _visitor ??= new HashVisitor();

        _visitor.Reset();
        _visitor.Visit(e);

        return _visitor.Hash.ToHashCode();
    }

    private sealed class HashVisitor : ExpressionVisitor {
        private HashCode _hash;

        internal HashCode Hash => _hash;

        internal void Reset() {
            _hash = new HashCode();
        }

        private void UpdateHash(int value) {
            _hash.Add(value);
        }

        private void UpdateHash(object? component) {
            var componentHash = NullHashValue;

            if(component != null) {
                var member = component as MemberInfo;
                if(member != null) {
                    componentHash = member.Name.GetHashCode();

                    var declaringType = member.DeclaringType;
                    if(declaringType != null && declaringType.AssemblyQualifiedName != null)
                        componentHash = (componentHash * 397) ^ declaringType.AssemblyQualifiedName.GetHashCode();
                } else {
                    componentHash = component.GetHashCode();
                }
            }

            _hash.Add(componentHash);
        }

        public override Expression Visit(Expression node) {
            if(node == null)
                return base.Visit(node);

            UpdateHash((int)node.NodeType);
            return base.Visit(node);
        }

        protected override Expression VisitConstant(ConstantExpression node) {
            UpdateHash(node.Value);
            return base.VisitConstant(node);
        }

        protected override Expression VisitMember(MemberExpression node) {
            UpdateHash(node.Member);
            return base.VisitMember(node);
        }

        protected override MemberAssignment VisitMemberAssignment(MemberAssignment node) {
            UpdateHash(node.Member);
            return base.VisitMemberAssignment(node);
        }

        protected override MemberBinding VisitMemberBinding(MemberBinding node) {
            UpdateHash((int)node.BindingType);
            UpdateHash(node.Member);
            return base.VisitMemberBinding(node);
        }

        protected override MemberListBinding VisitMemberListBinding(MemberListBinding node) {
            UpdateHash((int)node.BindingType);
            UpdateHash(node.Member);
            return base.VisitMemberListBinding(node);
        }

        protected override MemberMemberBinding VisitMemberMemberBinding(MemberMemberBinding node) {
            UpdateHash((int)node.BindingType);
            UpdateHash(node.Member);
            return base.VisitMemberMemberBinding(node);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node) {
            UpdateHash(node.Method);
            return base.VisitMethodCall(node);
        }

        protected override Expression VisitNew(NewExpression node) {
            UpdateHash(node.Constructor);
            return base.VisitNew(node);
        }

        protected override Expression VisitNewArray(NewArrayExpression node) {
            UpdateHash(node.Type);
            return base.VisitNewArray(node);
        }

        protected override Expression VisitParameter(ParameterExpression node) {
            UpdateHash(node.Type);
            return base.VisitParameter(node);
        }

        protected override Expression VisitTypeBinary(TypeBinaryExpression node) {
            UpdateHash(node.Type);
            return base.VisitTypeBinary(node);
        }
    }
}