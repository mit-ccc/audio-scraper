begin;
    insert into ingest.jobs
        (source_id)
    select
        ds.source_id
    from data.source ds
    where
        ds.auto_ingest;
commit;
