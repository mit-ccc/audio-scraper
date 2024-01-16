begin;
    insert into app.ingest_jobs
        (station_id)
    select
        ds.station_id
    from data.station ds
    where
        ds.auto_ingest;
commit;
