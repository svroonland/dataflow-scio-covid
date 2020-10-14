# dataflow-scio-covid

Example code for processing COVID data with [Google Dataflow](https://cloud.google.com/dataflow) using [Scio](https://spotify.github.io/scio/)

## Running 

```
sbt runMain nl.vroste.dataflow_scio_covid.Main --project=google-cloud-project-id --runner=DataflowRunner --zone=us-central1-a --region=us-central1 --input=gs://bucketname/RIVM_NL_municipal.csv --output=gs://bucketname/outputfolder
```

### REPL

To experiment with current codebase in [Scio REPL](https://github.com/spotify/scio/wiki/Scio-REPL)
simply:

```bash
sbt repl/run
```

---

This project is based on the [scio.g8](https://github.com/spotify/scio.g8).
