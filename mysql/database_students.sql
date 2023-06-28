use db;


create table students(
    studentid int not null AUTO_INCREMENT,
    firstname varchar(100) not null,
    lastname varchar(100) not null,
    PRIMARY KEY (studentid)
);

INSERT INTO students(firstname, lastname)
VALUES ("a","asaaa"),("ddd","aaaa");