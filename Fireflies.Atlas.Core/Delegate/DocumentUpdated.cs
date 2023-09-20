namespace Fireflies.Atlas.Core.Delegate;

public delegate void DocumentUpdated<in TDocument>(TDocument newDocument, TDocument deletedDocument);