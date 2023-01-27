# Migratable.PostgresProvider

Database provider for https://www.nuget.org/packages/Migratable to add support for *Postgres*.

## Installation

Both this provider and the main Migratable are on Nuget.

## Usage

See https://www.nuget.org/packages/Migratable for details.

``` cs
var provider = new Migratable.Providers.PostgresProvider("connection-string");
var migrator = new Migratable.Migrator(provider);
migrator.LoadMigrations("./migrations");
migrator.SetVersion(5);
```

---

## MIT Licence

Copyright (c) 2023 K Cartlidge.
See `LICENCE` file for details.
