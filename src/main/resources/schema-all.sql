DROP TABLE CUSTOMER IF EXISTS;
CREATE TABLE CUSTOMER  (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(255),
    gender VARCHAR(255),
    age INT,
    registered DATE,
    orders INT,
    spent REAL,
    job VARCHAR(255),
    hobbies VARCHAR(255),
    is_married BOOLEAN
);
INSERT INTO CUSTOMER ( first_name,email,age,gender,registered,orders,spent,is_married)
VALUES('ram','ram007@gmail.com',34,'male','2024-12-01',30,56,TRUE);
INSERT INTO CUSTOMER ( first_name,email,age,gender,registered,orders,spent,is_married)
VALUES('laxman','laxman007@gmail.com',34,'male','2024-12-01',40,56,TRUE);
INSERT INTO CUSTOMER ( first_name,email,age,gender,registered,orders,spent,is_married)
VALUES('karan','karan007@gmail.com',34,'male','2024-12-01',45,78,TRUE);
INSERT INTO CUSTOMER ( first_name,email,age,gender,registered,orders,spent,is_married)
VALUES('Unni','Unni007@gmail.com',34,'male','2024-12-01',23,56,FALSE);
INSERT INTO CUSTOMER ( first_name,email,age,gender,registered,orders,spent,is_married)
VALUES('Rahaman','Rahaman007@gmail.com',34,'male','2024-12-01',32,90,TRUE);
INSERT INTO CUSTOMER ( first_name,email,age,gender,registered,orders,spent,is_married)
VALUES('Ravan','Ravan007@gmail.com',34,'male','2024-12-01',30,56,TRUE);
INSERT INTO CUSTOMER ( first_name,email,age,gender,registered,orders,spent,is_married)
VALUES('Andakasur','Andakasur007@gmail.com',34,'male','2024-12-01',58,57,TRUE);
INSERT INTO CUSTOMER ( first_name,email,age,gender,registered,orders,spent,is_married)
VALUES('Vrishali','Vrishali007@gmail.com',34,'female','2024-12-01',30,59,TRUE);
INSERT INTO CUSTOMER ( first_name,email,age,gender,registered,orders,spent,is_married)
VALUES('Arjun','Arjun007@gmail.com',34,'male','2024-12-01',30,60,TRUE);