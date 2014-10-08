create database if not exists conductor;
use conductor;

create table applications(
    appid       int unsigned primary key auto_increment,
    appname     char(64)     not null unique,
    authkey     char(32)     not null,
    state       char(16)     not null,
    type        char(16)     not null,
    path        char(255)    not null,
    timestamp   timestamp
) engine=innodb auto_increment=100000;

create table hosts(
    appid int unsigned not null,
    ip    char(15)     not null,
    count int unsigned not null,
    primary key(appid, ip, count)
) engine=innodb;

create table pools(
    appid int unsigned not null,
    pool  char (32)    not null,
    ip    char(15)     not null,
    primary key(appid, pool, ip)
) engine=innodb;

create table workers(
    workername   char(64)     primary key,
    appid        int unsigned not null,
    state        char(16)     not null,
    input        mediumblob   not null,
    status       mediumblob,
    continuation mediumblob,
    created      timestamp default current_timestamp,
    timestamp    timestamp
) engine=innodb;

create table messages(
    msgid      bigint unsigned primary key auto_increment,
    appid      int unsigned not null,
    workername char(64) not null,
    pool       char(32) not null default 'default',
    state      char(16) not null,
    lock_ip    char(15),
    priority   tinyint unsigned not null default 128,
    code       char(64) not null,
    data       mediumblob,
    timestamp  timestamp default current_timestamp
) engine=innodb;

create index msg1 on messages(timestamp, state, appid, pool, lock_ip, priority);
create index msg2 on messages(appid, workername, timestamp, msgid);
