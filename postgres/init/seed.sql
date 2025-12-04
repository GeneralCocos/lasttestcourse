INSERT INTO public.source_table (
    col_01_int, col_02_int, col_03_num, col_04_txt, col_05_ts,
    col_06_int, col_07_int, col_08_num, col_09_txt, col_10_ts,
    col_11_int, col_12_int, col_13_num, col_14_txt, col_15_ts,
    col_16_int, col_17_int, col_18_num, col_19_txt, col_20_ts,
    col_21_int, col_22_int, col_23_num, col_24_txt, col_25_ts,
    col_26_int, col_27_int, col_28_num, col_29_txt, col_30_ts,
    col_31_int, col_32_int, col_33_num, col_34_txt, col_35_ts,
    col_36_int, col_37_int, col_38_num, col_39_txt, col_40_ts,
    col_41_int, col_42_int, col_43_num, col_44_txt, col_45_ts,
    col_46_int, col_47_int, col_48_num, col_49_txt, col_50_ts
)
SELECT
    (random()*1000000)::int,        -- col_01_int
    (random()*1000000)::int,        -- col_02_int
    (random()*1000)::numeric(18,4), -- col_03_num
    'txt_' || (random()*1000000)::int,
    now() - (random()*365 || ' days')::interval,

    (random()*1000000)::int,
    (random()*1000000)::int,
    (random()*1000)::numeric(18,4),
    'txt_' || (random()*1000000)::int,
    now() - (random()*365 || ' days')::interval,

    (random()*1000000)::int,
    (random()*1000000)::int,
    (random()*1000)::numeric(18,4),
    'txt_' || (random()*1000000)::int,
    now() - (random()*365 || ' days')::interval,

    (random()*1000000)::int,
    (random()*1000000)::int,
    (random()*1000)::numeric(18,4),
    'txt_' || (random()*1000000)::int,
    now() - (random()*365 || ' days')::interval,

    (random()*1000000)::int,
    (random()*1000000)::int,
    (random()*1000)::numeric(18,4),
    'txt_' || (random()*1000000)::int,
    now() - (random()*365 || ' days')::interval,

    (random()*1000000)::int,
    (random()*1000000)::int,
    (random()*1000)::numeric(18,4),
    'txt_' || (random()*1000000)::int,
    now() - (random()*365 || ' days')::interval,

    (random()*1000000)::int,
    (random()*1000000)::int,
    (random()*1000)::numeric(18,4),
    'txt_' || (random()*1000000)::int,
    now() - (random()*365 || ' days')::interval,

    (random()*1000000)::int,
    (random()*1000000)::int,
    (random()*1000)::numeric(18,4),
    'txt_' || (random()*1000000)::int,
    now() - (random()*365 || ' days')::interval,

    (random()*1000000)::int,
    (random()*1000000)::int,
    (random()*1000)::numeric(18,4),
    'txt_' || (random()*1000000)::int,
    now() - (random()*365 || ' days')::interval,

    (random()*1000000)::int,
    (random()*1000000)::int,
    (random()*1000)::numeric(18,4),
    'txt_' || (random()*1000000)::int,
    now() - (random()*365 || ' days')::interval
FROM generate_series(1, 1000);
