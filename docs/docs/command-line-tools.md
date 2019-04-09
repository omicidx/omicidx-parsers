# Command-line tools

## SRA tooling

Process for processing SRA data:

1. Download mirror files from SRA
2. Convert any xml files to json


To build the SRA data, use the `omicidx-cli` executable.

```
omicidx-cli download-mirror-files NCBI_Mirror_DIR....
```

A new directory will be created with xml files as well as necessary
text files.

To convert from xml to json:

```
cd NCBI_Mirror_DIR
omicidx-cli process-xml-entity study
omicidx-cli process-xml-entity sample
omicidx-cli process-xml-entity experiment
omicidx-cli process-xml-entity run
```

## Biosample

Biosample follows a similar process to SRA. 

1. Download the single Biosample xml file
2. Convert xml files to json

In short:

```
biosample-cli --help
```

```
biosample-cli download_biosample
```

The downloaded file is a gzipped xml file. The filename is used in the
next step. Note that gzip files are handled automatically.

```
biosample-cli biosample_to_json biosample.xml.gz | gzip > biosample.json.gz
```

