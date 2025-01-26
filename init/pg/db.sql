create extension if not exists "uuid-ossp";

create table if not exists person (
    id uuid default uuid_generate_v4() primary key,
    name varchar(256),
    age integer,
    address varchar(256),
    email varchar(256),
    phone_number varchar(256),
    registration_date timestamp with time zone,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    deleted_at timestamp with time zone
);
