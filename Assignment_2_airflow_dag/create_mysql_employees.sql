-- Create employees table
drop if exists drop database employees;
create database employees;
use employees;
CREATE TABLE employees (
    employee_id INT AUTO_INCREMENT PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    team VARCHAR(50),
    role VARCHAR(50),
    hire_date DATE
);

-- Create calls table (referencing employees)
CREATE TABLE calls (
    call_id INT AUTO_INCREMENT PRIMARY KEY,
    employee_id INT,
    call_time DATETIME,
    phone VARCHAR(20),
    direction ENUM('inbound', 'outbound'),
    status ENUM('completed', 'missed', 'dropped'),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);



INSERT INTO employees (full_name, team, role, hire_date) VALUES
('James Smith', 'Support Alpha', 'Senior Specialist', '2021-03-15'),
('Mary Johnson', 'Support Alpha', 'Junior Agent', '2023-01-10'),
('Robert Williams', 'Support Beta', 'Team Lead', '2020-05-22'),
('Patricia Brown', 'Support Gamma', 'Quality Analyst', '2022-11-01'),
('Jennifer Jones', 'Support Alpha', 'Agent', '2023-06-12'),
('Michael Garcia', 'Support Beta', 'Agent', '2022-02-28'),
('Linda Miller', 'Support Gamma', 'Senior Specialist', '2021-07-19'),
('Elizabeth Davis', 'Support Alpha', 'Agent', '2023-04-05'),
('Barbara Rodriguez', 'Support Beta', 'Agent', '2022-09-30'),
('Susan Martinez', 'Support Gamma', 'Team Lead', '2019-12-15'),
('Joseph Hernandez', 'Support Alpha', 'Junior Agent', '2023-08-20'),
('Thomas Lopez', 'Support Beta', 'Senior Specialist', '2021-11-11'),
('Charles Gonzalez', 'Support Gamma', 'Agent', '2022-03-14'),
('Christopher Wilson', 'Support Alpha', 'Quality Analyst', '2022-05-01'),
('Daniel Anderson', 'Support Beta', 'Agent', '2023-02-25'),
('Matthew Thomas', 'Support Gamma', 'Junior Agent', '2023-09-01'),
('Anthony Taylor', 'Support Alpha', 'Agent', '2021-10-10'),
('Mark Moore', 'Support Beta', 'Senior Specialist', '2020-08-15'),
('Donald Jackson', 'Support Gamma', 'Agent', '2022-01-20'),
('Steven Martin', 'Support Alpha', 'Team Lead', '2020-01-05'),
('Paul Lee', 'Support Beta', 'Junior Agent', '2023-07-14'),
('Andrew Perez', 'Support Gamma', 'Agent', '2022-06-30'),
('Joshua Thompson', 'Support Alpha', 'Agent', '2023-03-18'),
('Kenneth White', 'Support Beta', 'Quality Analyst', '2022-10-12'),
('Kevin Harris', 'Support Gamma', 'Senior Specialist', '2021-04-22'),
('Brian Sanchez', 'Support Alpha', 'Junior Agent', '2023-11-05'),
('George Clark', 'Support Beta', 'Agent', '2022-07-07'),
('Edward Ramirez', 'Support Gamma', 'Agent', '2021-12-01'),
('Ronald Lewis', 'Support Alpha', 'Agent', '2023-05-20'),
('Timothy Robinson', 'Support Beta', 'Team Lead', '2020-02-14'),
('Jason Walker', 'Support Gamma', 'Junior Agent', '2023-10-10'),
('Jeffrey Young', 'Support Alpha', 'Senior Specialist', '2021-09-25'),
('Ryan Allen', 'Support Beta', 'Agent', '2022-04-15'),
('Jacob King', 'Support Gamma', 'Quality Analyst', '2022-08-19'),
('Gary Wright', 'Support Alpha', 'Agent', '2023-02-10'),
('Nicholas Scott', 'Support Beta', 'Junior Agent', '2023-12-01'),
('Eric Torres', 'Support Gamma', 'Agent', '2022-01-11'),
('Stephen Nguyen', 'Support Alpha', 'Senior Specialist', '2021-05-30'),
('Jonathan Hill', 'Support Beta', 'Agent', '2023-06-05'),
('Larry Flores', 'Support Gamma', 'Team Lead', '2019-11-20'),
('Justin Green', 'Support Alpha', 'Junior Agent', '2023-04-25'),
('Scott Adams', 'Support Beta', 'Agent', '2022-03-10'),
('Brandon Nelson', 'Support Gamma', 'Agent', '2021-08-14'),
('Benjamin Baker', 'Support Alpha', 'Quality Analyst', '2022-07-22'),
('Samuel Hall', 'Support Beta', 'Senior Specialist', '2021-01-18'),
('Gregory Rivera', 'Support Gamma', 'Junior Agent', '2023-10-30'),
('Alexander Campbell', 'Support Alpha', 'Agent', '2022-11-15'),
('Frank Mitchell', 'Support Beta', 'Agent', '2023-01-05'),
('Raymond Carter', 'Support Gamma', 'Agent', '2022-05-25'),
('Jack Roberts', 'Support Alpha', 'Junior Agent', '2023-09-15');



