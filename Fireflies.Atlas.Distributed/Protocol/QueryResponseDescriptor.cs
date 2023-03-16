namespace Fireflies.Atlas.Distributed.Protocol;

public class QueryResponseDescriptor {
    public Guid Uuid { get; set; }
    public bool EnableNotifications { get; set; }
    public IEnumerable<object> Documents { get; set; }
}