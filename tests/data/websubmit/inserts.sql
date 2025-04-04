INSERT INTO users VALUES ('admin@admin.com', 'apikey000', 1);
INSERT INTO users VALUES ('student1@email.com', 'apikey111', 0);
INSERT INTO users VALUES ('student2@email.com', 'apikey222', 0);
INSERT INTO users VALUES ('student3@email.com', 'apikey333', 0);

INSERT INTO lectures VALUES (1, 'lecture 1');
INSERT INTO lectures(id) VALUES (2);

INSERT INTO questions (lecture_id, question_number, question) VALUES (1, 1, 'question 1 for lecture 1');
INSERT INTO questions (lecture_id, question_number, question) VALUES (1, 2, 'question 2 for lecture 1');
INSERT INTO questions (lecture_id, question_number, question) VALUES (2, 1, 'question 1 for lecture 2');

INSERT INTO presenters (lecture_id, email) VALUES (1, 'student1@email.com');
INSERT INTO presenters (lecture_id, email) VALUES (2, 'student2@email.com');

INSERT INTO answers VALUES ('a1', 'student1@email.com', 1, 'answer 1', '2020-01-01 01:01:01', 100);
INSERT INTO answers(id, email, question_id, answer, submitted_at) VALUES ('a2', 'student2@email.com', 1, 'answer 2', '2020-02-01 01:01:02');
INSERT INTO answers(id, email, question_id, answer, submitted_at) VALUES ('a3', 'student3@email.com', 1, 'answer 3', '2020-01-01 01:01:03');
INSERT INTO answers(id, email, question_id, answer, submitted_at) VALUES ('a4', 'student1@email.com', 2, 'answer 4', '2020-01-01 01:02:01');
INSERT INTO answers(id, email, question_id, answer, submitted_at) VALUES ('a5', 'student2@email.com', 2, 'answer 5', '2020-02-01 01:02:02');
INSERT INTO answers(id, email, question_id, answer, submitted_at) VALUES ('a6', 'student1@email.com', 3, 'answer 6', '2020-01-01 02:01:01');
