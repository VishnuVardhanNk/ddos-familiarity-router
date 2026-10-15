do $$
begin
  if not exists (select from pg_roles where rolname = 'web_reader') then
    create role web_reader nologin;
  end if;
end $$;

grant web_reader to ddos;
grant usage on schema public to web_reader;
grant select on all tables in schema public to web_reader;
alter default privileges for role ddos in schema public grant select on tables to web_reader;

notify pgrst, 'reload schema';
