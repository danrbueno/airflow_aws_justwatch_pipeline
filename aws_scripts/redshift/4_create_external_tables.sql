/* Table fct_titles */
create external table schema_justwatch.fct_titles (
    title_id          varchar ,
    object_type       varchar ,
    title             varchar ,
    short_description varchar ,
    release_year      smallint ,
    age_dertification varchar ,
    runtime           bigint ,
    full_path         varchar ,
    imdb_score        double precision ,
    imdb_votes        double precision ,
    tmdb_popolarity   double precision ,
    tmbd_score        double precision ,
    season_count      double precision
)
stored as parquet 
location '<YOUR_PARQUETS_LOCATION>';

/* Table dim_genres */
create external table schema_justwatch.dim_genres (
    genre_id varchar ,
    name varchar
)
stored as parquet 
location '<YOUR_PARQUETS_LOCATION>';

/* Table dim_streaming_services */
create external table schema_justwatch.dim_streaming_services (
    streaming_service_id int ,
    clear_name varchar ,
    slug varchar ,
    icon varchar 
)
stored as parquet 
location '<YOUR_PARQUETS_LOCATION>';

/* Table dim_titles_genres */
create external table schema_justwatch.dim_titles_genres (
    title_id varchar ,
    genre_id varchar
)
stored as parquet 
location '<YOUR_PARQUETS_LOCATION>';

/* Table dim_titles_production_countries */
create external table schema_justwatch.dim_titles_production_countries (
    title_id varchar ,
    country_code varchar
)
stored as parquet 
location '<YOUR_PARQUETS_LOCATION>';

/* Table dim_titles_streaming_services */
create external table schema_justwatch.dim_titles_streaming_services (
    title_streaming_service_id varchar ,
    standard_web_url varchar ,
    title_id varchar ,
    streaming_service_id int 
)
stored as parquet 
location '<YOUR_PARQUETS_LOCATION>';