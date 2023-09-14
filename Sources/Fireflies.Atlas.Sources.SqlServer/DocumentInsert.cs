namespace Fireflies.Atlas.Sources.SqlServer;

public delegate void DocumentInsert<in TDocument>(TDocument newDocument);