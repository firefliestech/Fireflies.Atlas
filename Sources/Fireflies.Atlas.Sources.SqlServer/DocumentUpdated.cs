namespace Fireflies.Atlas.Sources.SqlServer;

public delegate void DocumentUpdated<TDocument>(TDocument newDocument, Lazy<TDocument> deletedDocument);