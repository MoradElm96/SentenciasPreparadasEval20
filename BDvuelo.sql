create database vuelo;
use vuelo; 

CREATE TABLE Ciudades (
  Id int primary key,
  Ciudad varchar(20) not null,
  Pais varchar(20),
  Continente varchar(10)
) ENGINE=InnoDB;

CREATE TABLE Vuelos (
  Id int primary key,
  Numero int not null,
  Destino int not null,
  Fecha Date NOT NULL,
  foreign key (destino) references Ciudades(Id)
  on delete cascade on update cascade
) ENGINE=InnoDB;

insert into Ciudades values(100,'Nueva York','Estados Unidos','America');
insert into Ciudades values(110,'Rio de Janeiro','Brasil','America');
insert into Ciudades values(120,'Buenos Aires','Argentina','America');
insert into Ciudades values(130,'Lima','Peru','America');
insert into Ciudades values(200,'Pekin','China','Asia');
insert into Ciudades values(210,'Hong Kong','Japon','Asia');
insert into Ciudades values(300,'Nairobi','Kenia','Africa');
insert into Ciudades values(310,'Nueva York','Estados Unidos','Africa');
insert into Ciudades values(400,'Camberra','Australia','Oceania');
insert into Ciudades values(410,'Sidney','Nueva Gales del Sur','Oceania');
insert into Ciudades values(500,'Madrid','España','Europa');
insert into Ciudades values(510,'Paris','Francia','Europa');
insert into Ciudades values(520,'Roma','Italia','Europa');
insert into Ciudades values(530,'La Valeta','Malta','Europa');
insert into Ciudades values(540,'Lisboa','Portugal','Europa');



 insert into Vuelos values(1,1500,100,'2018-03-01');
 insert into Vuelos values(2,3700,120,'2018-03-03');
 insert into Vuelos values(3,1145,200,'2018-03-06');
 insert into Vuelos values(4,3300,410,'2018-03-03');
 insert into Vuelos values(5,2678,540,'2018-03-08');
 insert into Vuelos values(6,9456,510,'2018-03-01');
 insert into Vuelos values(7,2374,530,'2018-04-04');
 insert into Vuelos values(8,5643,400,'2018-03-15');
 insert into Vuelos values(9,7432,210,'2018-03-25');
 insert into Vuelos values(10,8732,110,'2018-04-20');
 insert into Vuelos values(11,3759,300,'2018-05-07');
 insert into Vuelos values(12,6321,130,'2018-03-02');