namespace Fireflies.Atlas.Sources.SqlServer.Monitor;

public delegate void DocumentUpdated<in TDocument>(TDocument newDocument, TDocument deletedDocument);