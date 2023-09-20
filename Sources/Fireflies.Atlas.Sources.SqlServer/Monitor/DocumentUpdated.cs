namespace Fireflies.Atlas.Sources.SqlServer.Monitor;

public delegate void DocumentUpdated<TDocument>(TDocument newDocument, Lazy<TDocument> deletedDocument);