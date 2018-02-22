---
date: 2018-02-22T15:54:09
title: The Omix Index Project
type: index
weight: 0
---

## Overview

Databases and repositories of large-scale high-throughput are
incredibly valuable. Despite considerable successes in exposing the
information in these databases and repositories through searchable web
portals and programmatic APIs, there are additional use cases that
rely on access to bulk metadata. The [data engineering] required to
produce such bulk metadata can be significant and is often done by
individuals or groups to meet internal needs. The goal of the *Omix
Index Project* is provide metadata from large data repositories of
biological datasets in pre-digested formats that are immediately
amenable to data mining, natural language processing, loading to
databases or data warehouses, or for highly selective and flexible
search and query capabilities using state-of-the-art data science tools.


[data engineering]: https://blog.insightdatascience.com/data-science-vs-data-engineering-62da7678adaa

## Basic approach

In short, we access and process metadata from APIs or from
downloadable files to transform the data into one or more of the
following formats: 

- [Parquet]
- [Newline-delimited JSON]
- tab-separated [TSV] or comma-separated [CSV] files where applicable
- [Avro]

We then provide cookbook-style documentation for accessing and using
the resulting data products.

[Parquet]: https://parquet.apache.org/
[Newline-delimited JSON]: http://ndjson.org/
[TSV]: https://en.wikipedia.org/wiki/Tab-separated_values
[CSV]: https://en.wikipedia.org/wiki/Comma-separated_values
[Avro]: https://avro.apache.org/docs/current/


## Data resources

| Database  |   Status |
|-----------|----------|
| NCBI SRA  | Complete |
| NCBI GEO  | In progress |
| NCI GDC   | Planned | 

## Features

- Beautiful, readable and very user-friendly design based on Google's material
  design guidelines, packed in a full responsive template with a well-defined
  and [easily customizable color palette]({{< relref "getting-started/index.md#changing-the-color-palette" >}}), great typography, as well as a
  beautiful search interface and footer.

- Well-tested and optimized Javascript and CSS including a cross-browser
  fixed/sticky header, a drawer that even works without Javascript using
  the [checkbox hack](http://tutorialzine.com/2015/08/quick-tip-css-only-dropdowns-with-the-checkbox-hack/) with fallbacks, responsive tables that scroll when
  the screen is too small and well-defined print styles.

- Extra configuration options like a [project logo]({{< relref "getting-started/index.md#adding-a-logo" >}}), links to the authors
  [GitHub and Twitter accounts]({{< relref "getting-started/index.md#adding-a-github-and-twitter-account" >}}) and display of the amount of stars the
  project has on GitHub.

- Web application capability on iOS – when the page is saved to the homescreen,
  it behaves and looks like a native application.

See the [getting started guide]({{< relref "getting-started/index.md" >}}) for instructions how to get
it up and running.

## Acknowledgements

Last but not least a big thank you to [Martin Donath](https://github.com/squidfunk). He created the original [Material theme](https://github.com/squidfunk/mkdocs-material) for Hugo's companion [MkDocs](http://www.mkdocs.org/). This port wouldn't be possible without him.

Furthermore, thanks to [Steve Francia](https://gihub.com/spf13) for creating Hugo and the [awesome community](https://github.com/spf13/hugo/graphs/contributors) around the project.
