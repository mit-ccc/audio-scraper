/*
 * Primary data tables
 */

drop schema if exists data cascade;
create schema data;

drop table if exists data.station cascade;
create table data.station
(
    station_id integer not null primary key,

    name text not null unique,
    stream_url text not null unique, -- no reason to allow the same url twice
    auto_ingest bool not null default false,
    lang char(2), -- null => autodetect for transcribe
    retry_on_close bool not null default false
);

/*
 * Ingest tables
 */

drop schema if exists ingest cascade;
create schema ingest;

drop table if exists ingest.jobs cascade;
create table ingest.jobs
(
    station_id integer not null primary key
               references data.station
               on delete restrict,

    is_locked bool not null default false,
    create_dt timestamptz not null default now(),
    error_count integer not null default 0,
    last_error text
);

/*
 * Transcribe tables
 */

drop schema if exists transcribe cascade;
create schema transcribe;

drop table if exists transcribe.jobs cascade;
create table transcribe.jobs
(
    chunk_id bigserial not null primary key,

    station_id integer not null
               references data.station
               on delete restrict,

    create_dt timestamptz not null default now(),
    url text not null,
    error_count integer not null default 0,
    last_error text
);

create index idx_jobs_station_id on transcribe.jobs(station_id);
