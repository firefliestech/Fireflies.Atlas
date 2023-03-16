# Fireflies Atlas Core

Fireflies Atlas is a database agnostic caching layer that can be used with miscellaneous data sources such as Redis and Sql Server.

## Before the example
Atlas is all about documents, below is the document descriptors used in the example below.
```
public class BookshelfDocument {
    [AtlasField]
    [AtlasKey]
    public virtual int BookshelfId { get; set; }

    [AtlasField]
    public virtual string Name { get; set; }

    public virtual IEnumerable<ShelfDocument> Shelfs { get; set; }
}

public class ShelfDocument {
    [AtlasField]
    [AtlasKey]
    public virtual int ShelfId { get; set; }

    [AtlasField]
    public virtual int BookshelfId { get; set; }

    [AtlasField]
    public virtual int Index { get; set; }

    public virtual IEnumerable<BookDocument> Books { get; set; }
}

public class BookDocument {
    [AtlasField]
    [AtlasKey]
    public virtual int BookId { get; set; }

    [AtlasField]
    public virtual string Name { get; set; }

    [AtlasField]
    public virtual string Author { get; set; }

    public virtual LendingStateDocument LendingState { get; set; }
}

public class LendingStateDocument {
    [AtlasKey]
    public virtual int BookId { get; set; }

    public virtual string State { get; set; }
    public virtual DateTimeOffset At { get; set; }
}
```

## Example
Everything starts with the AtlasBuilder.
```
var builder = new AtlasBuilder();
```
Then we need to add some sources
```
var sqlServerSource = builder.Create(a => new SqlServerSource(a, "server=.; Database=my_database; user=sa; password=my_password;Trust Server Certificate=true"));
var redisSource = builder.Create(a => new RedisSource(a, "localhost"));
```
From here we can start to describe our documents and their relationships.

In this example the hierarchy is like this:
- Bookshelf (SQL), "root" document
    - Shelfs (SQL), a bookshelf has a list of shelfs
        - Book (SQL), a book on a shelf
            - Lending state (Redis), whats the lending state of the book? 

Above you can see that we mix SQL and redis sources but the resulting document will be agnostic to this.

Below is how we would describe this in code.
```
builder.AddDocument<BookshelfDocument>()
    .SqlServerSource(sqlServerSource, "dbo", "bookshelf")
    .PreloadDocuments()
    .AddRelation<ShelfDocument>(x => x.Shelfs, (document, foreign) => document.BookshelfId == foreign.BookshelfId)
    .AddIndex(x => x.BookshelfId);

builder.AddDocument<ShelfDocument>()
    .SqlServerSource(sqlServerSource, "dbo", "shelfs")
    .PreloadDocuments()
    .AddRelation<BookDocument>(x => x.Books, (document, foreign) => document.ShelfId == foreign.ShelfId)
    .AddIndex(x => x.ShelfId)
    .AddIndex(x => x.BookshelfId);

builder.AddDocument<BookDocument>()
    .SqlServerSource(sqlServerSource, "dbo", "book")
    .PreloadDocuments()
    .AddRelation<LendingStateDocument>(x => x.LendingState, (document, foreign) => document.BookId == foreign.BookId)
    .AddIndex(x => x.BookId)
    .AddIndex(x => x.ShelfId);

builder.AddDocument<LendingStateDocument>()
    .RedisHashSource(redisSource, 1, "books:lendingstate");
```

Now we are ready to build the atlas.
```
var atlas = await builder.Build();
```

Lets query atlas.
```
var document = await atlas.GetDocument<BookshelfDocument>(f => f.Name.Contains("Bruks"));
Console.WriteLine($"{document.Name} has {document.Shelfs.Count()} shelfs");
```

## Linq provider
A simple LINQ provider would be implemented like this
```
public class AtlasQuery : AtlasContext {
    public AtlasQuery(Core.Atlas atlas) : base(atlas) {
    }

    public IQueryable<BookshelfDocument> Bookshelfs => CreateQueryable<BookshelfDocument>();
}
```

We can then use it like any other LINQ IQueryable-object.
```
var bookshelf = atlasQuery.Bookshelfs
    .First(y => y.Name.Contains("Bruks"));
Console.WriteLine(bookshelf.Name);
```

_Logo by freepik_