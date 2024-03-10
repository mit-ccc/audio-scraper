begin;
    insert into ingest.jobs
        (station_id)
    select
        ds.station_id
    from data.station ds
    where
        ds.auto_ingest;
commit;
