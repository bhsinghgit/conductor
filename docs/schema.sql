create database if not exists shepherd;
use shepherd;

create table apps(
    appid     int unsigned primary key auto_increment,
    authkey   char(32)  not null,
    state     char(16)  not null,
    path      char(255) not null,
    timestamp timestamp
) engine=innodb auto_increment=100000;

create table appname(
    appname char(64) primary key,
    appid   int unsigned,
    foreign key(appid) references apps(appid)
) engine=innodb;

create table hosts(
    appid       int unsigned not null,
    ip          char(15)     not null,
    count_async int unsigned not null,
    count_sync  int unsigned not null,
    primary key(appid, ip)
) engine=innodb;

create table pools(
    appid int unsigned not null,
    pool  char(32)     not null,
    ip    char(15)     not null,
    primary key(appid, pool, ip)
) engine=innodb;

create table locks(
    sequence   bigint unsigned primary key auto_increment,
    lockname   char(64)        not null,
    appid      int unsigned    not null,
    workerid   bigint unsigned not null,
    unique(lockname, appid, workerid)
) engine=innodb;
create index lock1 on locks(appid, workerid, lockname);

create table workers(
    workerid     bigint unsigned primary key auto_increment,
    appid        int unsigned not null,
    state        char(16)     not null,
    status       longblob,
    continuation longblob,
    session      int unsigned not null default 0,
    created      timestamp default current_timestamp,
    timestamp    timestamp,
    foreign key(appid) references apps(appid)
) engine=innodb;

create table workername(
    appid      int unsigned not null,
    workername char(64)     not null,
    workerid   bigint unsigned,
    primary key(appid, workername),
    foreign key(workerid) references workers(workerid)
) engine=innodb;

create table messages(
    msgid          bigint unsigned primary key auto_increment,
    appid          int unsigned    not null,
    workerid       bigint unsigned not null,
    senderappid    int unsigned not null,
    senderworkerid bigint unsigned not null,
    pool           char(32) not null default 'default',
    state          char(16) not null,
    lock_ip        char(15),
    priority       tinyint unsigned not null default 128,
    code           char(64) not null,
    data           longblob,
    timestamp      timestamp default current_timestamp
) engine=innodb;

create index msg1 on messages(timestamp, state, appid, pool, lock_ip, priority);
create index msg2 on messages(appid, workerid, timestamp, msgid);

insert into apps set
    authkey='testkey',
    state='active',
    path='/tmp/abcd/bin/python';

insert into appname set
    appname='testapp',
    appid=100000;

insert into hosts set
    appid=100000,
    ip='127.0.0.1',
    count_async=100,
    count_sync=0;
