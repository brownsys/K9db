CREATE VIEW city_doctor_count AS '"SELECT address_doctors.city, COUNT(*) AS `count` FROM address_doctors GROUP BY address_doctors.city"';
CREATE VIEW city_patient_count AS '"SELECT address_patients.city, COUNT(*) AS `count` FROM address_patients GROUP BY address_patients.city"';
CREATE VIEW patient_info AS '"SELECT * FROM patients INNER JOIN address_patients ON patients.id = address_patients.id"';
CREATE VIEW doctor_info AS '"SELECT * FROM doctors INNER JOIN address_doctors ON doctors.id = address_doctors.id"';

-- Nested Views
CREATE VIEW doctor_info_city_count AS '"SELECT * FROM doctor_info JOIN city_doctor_count ON doctor_info.city = city_doctor_count.city"';
