CREATE TABLE public.dim_countries (
    name character varying(256) ENCODE lzo,
    alpha2_code character varying(256) ENCODE lzo,
    alpha3_code character varying(256) ENCODE lzo,
    flag character varying(256) ENCODE lzo
) DISTSTYLE AUTO;