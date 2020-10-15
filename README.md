# dataflow-scio-covid

Example code for processing COVID data with [Google Dataflow](https://cloud.google.com/dataflow) using [Scio](https://spotify.github.io/scio/)

## Running 

* Get the input data:

```
curl -L https://github.com/J535D165/CoronaWatchNL/raw/master/data-geo/data-municipal/RIVM_NL_municipal.csv > RIVM_NL_municipal.csv
```

* Copy it to Google Storage

```
gsutil cp RIVM_NL_municipal.csv gs://steven-dataworkz-scio
```

```
sbt runMain nl.vroste.dataflow_scio_covid.Main  
    --project=google-cloud-project-id 
    --runner=DataflowRunner 
    --zone=us-central1-a
    --region=us-central1 
    --input=gs://bucketname/RIVM_NL_municipal.csv 
    --output=gs://bucketname/outputfolder
```

Run the R notebook in `notebook.Rmd` in RStudio to produce plots.

---

This project is based on the [scio.g8](https://github.com/spotify/scio.g8).
