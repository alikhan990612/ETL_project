--Создаю таблицу куда буду заполнять данные из Spotify
create table wt_spotify_pt
(
data_id number,
music_name varchar2(255),
played_at date,
release_date date,
CONSTRAINT data_id_pk PRIMARY KEY (data_id)
);

--Создаю sequence для data_id 
create sequence sq_spotify
start with 1 
maxvalue 9999 
minvalue 1
nocycle
nocache
noorder;

--Создаю триггер где data_id будет автоматом брать новое значение из sequence
create or replace trigger trg_spotify
before insert on wt_spotify_pt 
referencing new as new old as old
for each row
declare 
begin
    select sq_spotify.nextval into :new.data_id from dual;
end trg_spotify;

--Создаю таблицу логирования
create table s_log_wt_spotify_pt(
data_id number,
music_name varchar2(255),
played_at date,
release_date date,
user_n_dml varchar2(60),
deistvie_dml varchar2(10),
dat_dml date
);

--Создаю триггер after dml, чтобы фиксировать все изменения в таблице wt_spotify_pt
create or replace trigger wt_spotify_pt_log
after insert or update or delete on wt_spotify_pt
referencing new as new old as old
for each row 
begin
if inserting then
insert into s_log_wt_spotify_pt (data_id, music_name, played_at, release_date, user_n_dml, deistvie_dml, dat_dml)
values(:new.data_id, :new.music_name, :new.played_at, :new.release_date,user,'1',sysdate);
end if;
if updating then
insert into s_log_wt_spotify_pt (data_id, music_name, played_at, release_date, user_n_dml, deistvie_dml, dat_dml)
values(:new.data_id, :new.music_name, :new.played_at, :new.release_date,user,'2',sysdate);
end if;
if deleting then
insert into s_log_wt_spotify_pt (data_id, music_name, played_at, release_date, user_n_dml, deistvie_dml, dat_dml)
values(:old.data_id, :old.music_name, :old.played_at, :old.release_date,user,'3',sysdate);
end if;
end wt_spotify_pt_log;

