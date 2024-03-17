# Audio scraper
---

This application ingests audio from online streams or files, saves it to either
an S3 bucket or a local folder, and transcribes it. Various parts of
this ingestion process are configurable: the bucket, the number of workers, the
transcription quality and whether to use GPUs, and more. The output transcripts
include not only transcribed text but also diarized speaker IDs and word-level
time alignments. (Transcription is via OpenAI's Whisper models and the excellent
[WhisperX](https://github.com/m-bain/whisperX) package.)

There's support for many kinds of online streams, including direct aac/mp3/etc
streams and playlists like .pls and .m3u. There's also support for webscrape
streams where direct or playlist URLs have to be extracted from a page. Out of
the box only iHeartRadio streams are webscrape-able; if you want others you'll
have to write a bit of code that says how get stream URLs from the page.

The app is packaged with Kubernetes for both local use via minikube and an easy
migration path to a larger deployment on a K8s cluster.

### Setup
To get started, you need to do a few things:
* For local use, install [docker](https://docs.docker.com/engine/install/) and
  [minikube](https://minikube.sigs.k8s.io/docs/start/).
* Go to Huggingface and accept the access conditions for certain gated models.
  They're available for free, but you have to agree to share your contact info
  to indicate you use them. (This helps the developers apply for grants.)
    * [pyannote/speaker-diarization-3.1](https://huggingface.co/pyannote/speaker-diarization-3.1)
    * [pyannote/segmentation-3.0](https://huggingface.co/pyannote/segmentation-3.0)
* Create `secrets.env`, a simple key=value list of environment variables, one
  per line, with the following keys:
    * `POSTGRES_PASSWORD`, which is the password used to access the coordinating
      database container. Pick whatever you like.
    * `HF_TOKEN`, a [Huggingface access token](https://huggingface.co/docs/hub/en/security-tokens)
      that allows the app to pull the gated models you signed up for above.
    * If audio and transcripts are written to S3, you also need to provide
      `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
* Create the list of audio sources to ingest. This is a tab-separated file
  that needs to be at the path `images/postgres/data/source-data.csv`. (See
  `images/postgres/data/station-data-example.csv` for an example.) It should
  contain 6 columns (in this order):
    * `source_id`, an arbitrary integer ID you can assign;
    * `name`, an arbitrary text name for the source (e.g., for a radio station,
      its callsign is a good choice).
    * `stream_url`, the URL to scrape;
    * `auto_ingest`, whether to start ingesting this source immediately on
      startup. If `False`, you'll have to manually run SQL against the database
      container to start ingesting this source.
    * `lang`, the two-letter ISO code for the language of the audio.
      If null, the language will be autodetected, but errors in language
      detection may reduce transcription quality.
    * `retry_on_close`, whether to reopen the connection if the server closes it
      as complete. Some ongoing streams are misconfigured and will do this even
      if there's more audio available on reconnecting.
* Edit some settings:
  * The configuration map at the top of `deploy.yaml` should have your preferred
    settings, especially the storage location to write audio and transcripts to.
  * The number of replicas for the ingest and transcribe deployments in
    `deploy.yaml` may need to be increased or decreased, depending on how many
    sources you're ingesting.
  * Finally, for local use you may need to experiment with the `cpus` and
    `memory` arguments that define the minikube cluster resource allocations.
    The requested and maximum CPU/memory usage for each container are marked in
    the `deploy.yaml` file so that you can calculate how much your deployments
    will need.

## Usage
To start up locally one machine, you can use the provided Makefile and
deploy.yaml. Run `make start` to create a minikube cluster, and then `make` or
equivalently `make up` to deploy the application to it. You can monitor it by
examining the files it writes, or using Kubernetes' `kubectl` command. Minikube
also supports the Kubernetes dashboard for a graphical interface -- see the
Makefile. You'll need to allow a few minutes on startup for the application to
begin working; the transcribe containers in particular can take as long as 10
minutes to load their models and start processing audio. (Note that if you have
too few transcribe containers for the sources being ingested, you can run out of
disk space!)

For deployment to a larger, preexisting Kubernetes cluster, you'll need to make
some changes to the manifest and shouldn't need the Makefile. If you need to do
this, you already either know your way around devops or have access to someone
who does, so we won't say more about it here. Note that there is a possibly
helpful script provided at `bin/ecr.sh` to build the container images and push
them to Amazon ECR repositories in case you're using EKS.

## Live editing of sources
To edit the set of sources marked for ingest once the app is running, there are
two things you can do:
1. Stop the app with `make down`, edit the set of sources (or change their
   `auto_ingest` flags) and redeploy, or
2. Run a bit of SQL. First, connect to the PostgreSQL database container. Load
   source information into the data.sources table in the same format as in the
   CSV file, and insert corresponding rows into the ingest.jobs table. If you
   want to stop ingesting something that's already defined in data.sources, just
   delete the appropriate ingest.jobs row. Jobs which are no longer present will
   stop being ingested, and new ones will be picked up by a worker (if any are
   free - you may need to scale out the ingest and/or transcribe deployments).

## Output format
The ingest workers will transcode their input audio to WAV format before saving
it, for uniformity's sake. Audio is saved in chunks of the same duration, rather
than the same input file size, with the default being 30 seconds. Audio files
are removed by default after being transcribed, to save space and reduce costs,
but you can keep them by setting the `REMOVE_AUDIO` configuration in
`deploy.yaml` to 'false'.

The transcribe workers use [WhisperX](https://github.com/m-bain/whisperX) and
also perform diarization and word-level alignment. The output `.json.gz` files
have diarized speaker IDs and word-level timestamps, as well as the transcribed
text.
