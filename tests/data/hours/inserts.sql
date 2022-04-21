INSERT INTO Profile VALUES ("Joe Miller", "joe_miller@gmail.com", "14013890221", "https://joe_miller_pic", 0, "he/him", "https://brown.zoom.us/j/19283746900");
INSERT INTO Profile VALUES ("Anna Anderson", "anna_anderson@brown.edu", "12032231021", "https://anna_anderson_pic", 0, "she/her", "https:brown.zoom.us/j/09827368398");
INSERT INTO Profile VALUES ("Sarah Chin", "sarah_chin@brown.edu", "18273881001", "https://sarah_chin_pic", 0, "she/her", "");

INSERT INTO CoursePermissions VALUES ("1", "1", "joe_miller@gmail.com", "STAFF");
INSERT INTO CoursePermissions VALUES ("2", "2", "anna_anderson@brown.edu", "STAFF");

INSERT INTO Notifications VALUES ("1", "sarah_chin@brown.edu");

INSERT INTO User VALUES ("joe_miller@gmail.com", "1", 0, "2021-09-21 12:22:21", "2022-04-19 16:59:00");
INSERT INTO User VALUES ("anna_anderson@brown.edu", "2", 0, "2020-08-01 01:11:19", "2021-11-22 02:30:19");
INSERT INTO User VALUES ("sarah_chin@brown.edu", "3", 0, "2020-02-17 21:08:10", "2022-04-19 16:48:29");

INSERT INTO Notification VALUES ("1", "You've been claimed", "You've been claimed by TA Joe Miller", "2022-04-19 18:01:05", "CLAIMED");

INSERT INTO Course VALUES ("1", "Computer Vision", "1430", "Spring 2022", 0);
INSERT INTO Course VALUES ("2", "Introduction to Discrete Structures and Probability", "0220", "Spring 2022", 0);

INSERT INTO CourseInvite VALUES ("taylor_johnson@gmail.com", "1", "");
INSERT INTO CourseInvite VALUES ("john_garcia@brown.edu", "2", "STAFF");

INSERT INTO Queue VALUES ("1", "TA Hours", "Joe's conceptual hours for cs1430", "CIT 230", "2022-04-19 19:00:00", 0, 1, "1", 0);

INSERT INTO Tickets VALUES ("1", "1");

INSERT INTO TicketUserdata VALUES ("3", "sarah_chin@brown.edu", "https://sarah_chin_pic", "Sarah Chin", "she/her");

INSERT INTO Ticket VALUES ("1", "3", "1", "2022-04-19 17:00:19", "2022-04-19 18:01:05", "1", "MISSING", "I have a bad bug", 1);