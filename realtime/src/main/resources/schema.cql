create table location( location text, hour int, day int ,month int, count int, primary key(location,hour,day,month));

create table tag( tag text, hour int, day int ,month int, count int, primary key(tag,hour,day,month));

create table geo( lat decimal, lng decimal, hour int, day int ,month int,primary key(lat,lng));

	