CREATE TABLE Profile ( \
    DisplayName text, \
    Email text, \
    PhoneNumber varchar(50), \
    PhotoURL text, \
    IsAdmin int, \
    Pronouns text, \
    MeetingLink text, \
    PRIMARY KEY(Email) \
);

--could also be done with a compound key
CREATE TABLE CoursePermissions ( \
    ID varchar(255), \
    CourseID varchar(255), \
    Email text, \
    CoursePermission varchar(50), \
    PRIMARY KEY(ID), \
    FOREIGN KEY (Email) REFERENCES Profile(Email), \
    FOREIGN KEY (CourseID) REFERENCES Course(ID) \
);

CREATE TABLE Notifications ( \
    NotificationID varchar(255), \
    Email text, \
    PRIMARY KEY(NotificationID), \
    FOREIGN KEY (Email) REFERENCES Profile(Email), \
    FOREIGN KEY (NotificationID) REFERENCES Notification(ID) \
);

CREATE TABLE User ( \
    OWNS_Email text, \
    PII_ID varchar(255), \
    Disabled int, \
    CreationTimestamp datetime, \
    LastLogInTimestamp datetime, \
    PRIMARY KEY(PII_ID), \
    FOREIGN KEY (OWNS_Email) REFERENCES Profile(Email) \
);

CREATE TABLE Notification ( \
    ID varchar(255), \
    Title text, \
    Body text, \
    Timestamp datetime, \
    Type varchar(50), \
    PRIMARY KEY(ID) \
);

CREATE TABLE Course ( \
    ID varchar(255), \
    Title text, \
    Code text, \
    Term text, \
    IsArchived int, \
    PRIMARY KEY(ID) \
);

--fix later email is not unique
CREATE TABLE CourseInvite ( \
    PII_Email text, \
    CourseID varchar(255), \
    Permission varchar(50), \
    PRIMARY KEY(PII_Email), \
    FOREIGN KEY (CourseID) REFERENCES Course(ID) \
);

CREATE TABLE Queue ( \
    ID varchar(255), \
    Title text, \
    Description text, \
    Location text, \
    EndTime datetime, \
    ShowMeetingLinks int, \
    AllowTicketEditing int, \
    CourseID varchar(255), \
    IsCutOff int, \
    PRIMARY KEY(ID), \
    FOREIGN KEY (CourseID) REFERENCES Course(ID) \
);

CREATE TABLE TicketUserdata ( \
    UserID varchar(255), \
    Email text, \
    PhotoURL text, \
    DisplayName text, \
    Pronouns text, \
    PRIMARY KEY(Email), \
    FOREIGN KEY (UserID) REFERENCES User(ID) \
);

CREATE INDEX TicketUserdata_UserID ON TicketUserdata(UserID);

CREATE TABLE Ticket ( \
    ID varchar(255), \
    OWNER_User varchar(255), \
    Queue varchar(255), \
    CreatedAt datetime, \
    ClaimedAt datetime, \
    ACCESSOR_ClaimedBy varchar(255), \
    Status varchar(50), \
    Description text, \
    Anonymize int, \
    FOREIGN KEY (OWNER_User) REFERENCES TicketUserdata(UserID), \
    FOREIGN KEY (ACCESSOR_ClaimedBy) REFERENCES User(ID), \
    FOREIGN KEY (Queue) REFERENCES Queue(ID), \
    PRIMARY KEY(ID) \
);

CREATE TABLE Tickets ( \
    QueueID varchar(255), \
    TicketID varchar(255), \
    PRIMARY KEY (TicketID), \
    FOREIGN KEY (QueueID) REFERENCES Queue(ID), \
    FOREIGN KEY (TicketID) REFERENCES Ticket(ID) \
);