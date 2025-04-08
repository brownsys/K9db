CREATE VIEW lectures_with_question_counts AS '" \
( \
    SELECT lectures.id AS id, lectures.label, 0 AS U_c \
    FROM lectures LEFT JOIN questions ON (lectures.id = questions.lecture_id) \
    WHERE questions.id IS NULL \
    GROUP BY lectures.id, lectures.label \
) \
UNION \
( \
    SELECT lectures.id AS id, lectures.label, COUNT(*) AS U_c \
    FROM lectures JOIN questions ON (lectures.id = questions.lecture_id) \
    GROUP BY lectures.id, lectures.label \
) \
ORDER BY id \
"';
CREATE VIEW answers_with_apikey AS '" \
SELECT users.apikey, answers.answer, answers.submitted_at, answers.grade \
FROM answers JOIN users ON (answers.email = users.email) \
ORDER BY answers.submitted_at \
"';
CREATE VIEW grade_avg AS '"\
SELECT email, AVG(grade) \
FROM answers \
GROUP BY email \
"';
